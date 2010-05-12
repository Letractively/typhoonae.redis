# -*- coding: utf-8 -*-
#
# Copyright 2010 Tobias Rodäbel
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""TyphoonAE's Datastore implementation using Redis as backend.

This code reuses substantial portions of the datastore_file_stub.py from the
Google App Engine SDK.

Unlike the file stub's implementation it is designed to handle larger amounts
of production data and concurrency.
"""

from datetime import datetime

from google.appengine.api import apiproxy_stub
from google.appengine.api import datastore
from google.appengine.api import datastore_errors
from google.appengine.api import datastore_types
from google.appengine.datastore import datastore_index
from google.appengine.datastore import datastore_pb
from google.appengine.datastore import entity_pb
from google.appengine.ext.db import Key
from google.appengine.runtime import apiproxy_errors

import hashlib
import indexes
import logging
import redis
import string
import sys
import threading
import time
import uuid


entity_pb.Reference.__hash__      = lambda self: hash(self.Encode())
datastore_pb.Query.__hash__       = lambda self: hash(self.Encode())
datastore_pb.Transaction.__hash__ = lambda self: hash(self.Encode())

# Constants
_MAXIMUM_RESULTS      = 1000
_MAX_QUERY_OFFSET     = 1000
_MAX_QUERY_COMPONENTS = 100
_MAX_TIMEOUT          = 30

_CURSOR_CONCAT_STR = '!CURSOR!'
_PATH_CONCAT_STR = '\x07'

_DATASTORE_OPERATORS = {
    datastore_pb.Query_Filter.LESS_THAN:             '<',
    datastore_pb.Query_Filter.LESS_THAN_OR_EQUAL:    '<=',
    datastore_pb.Query_Filter.GREATER_THAN:          '>',
    datastore_pb.Query_Filter.GREATER_THAN_OR_EQUAL: '>=',
    datastore_pb.Query_Filter.EQUAL:                 '==',
}

_PROPERTY_VALUE_TYPES = ['int', 'float', 'str', 'unicode']
_REDIS_SORT_ALPHA_TYPES = frozenset(['str', 'unicode', 'datetime'])

# Reserved Redis keys
_ENTITY_GROUP_LOCK = '%(app)s!%(entity_group)s:\vLOCK'
_KIND_INDEX        = '%(app)s!%(kind)s:\vALL_KEYS'
_NEXT_ID           = '%(app)s!\vNEXT_ID'
_PROPERTY_INDEX    = '%(app)s!%(kind)s:%(prop)s:%(encval)s:\vKEYS'
_PROPERTY_TYPES    = '%(app)s!%(kind)s:%(prop)s:\vTYPES'
_PROPERTY_VALUE    = '%(key)s:%(prop)s'
_TEMPORARY_KEY     = '%(app)s!TEMP:%(uuid)s'


class _StoredEntity(object):
    """Entity wrapper.

    Provides three variants of the same entity for various stub operations.
    """

    def __init__(self, entity):
      """Constructor.

      Args:
          entity: entity_pb.EntityProto to store.
      """
      assert isinstance(entity, entity_pb.EntityProto)
      self.__protobuf = entity

    @property
    def protobuf(self):
        """Return native protobuf Python object."""

        return self.__protobuf

    @property
    def encoded_protobuf(self):
        """Return encoded binary representation of above protobuf."""

        return self.__protobuf.Encode()

    @property
    def native(self):
        """Return datastore.Entity instance."""

        return datastore.Entity._FromPb(self.__protobuf)

    def key(self):
        """Return a entity_pb.Reference instance."""

        return self.__protobuf.key()


class QueryCursor(object):
    """A query cursor.

    Public properties:
        cursor: the integer cursor
        count: the original total number of results
        keys_only: whether the query is keys_only
        app: the app for which this cursor was created

    Class attributes:
        _next_cursor: the next cursor to allocate
        _next_cursor_lock: protects _next_cursor
        _offset: the internal index for where we are in the results
        _limit: the limit on the original query, used to track remaining results
    """
    _next_cursor = 1
    _next_cursor_lock = threading.Lock()

    def __init__(self, db, query, results):
        """Constructor.

        Args:
            db: The Redis database connection.
            query: Query request protocol buffer instance.
            results: A list of datastore.Entity instances.
        """

        self.__db = db

        offset = 0
        cursor_entity = None

        if (query.has_compiled_cursor()
                and query.compiled_cursor().position_list()):
            (cursor_entity, inclusive) = self._DecodeCompiledCursor(
                    query, query.compiled_cursor())

        if query.has_offset():
            offset += query.offset()

        if offset > 0:
            self.__last_result = results[min(len(results), offset) - 1]
        else:
            self.__last_result = cursor_entity

        self.__results = results

        self.__query = query
        self.__offset = 0

        self.app = query.app()
        self.keys_only = query.keys_only()
        self.count = len(self.__results)
        self.cursor = self._AcquireCursorID()

    def _AcquireCursorID(self):
        """Acquires the next cursor id in a thread safe manner."""
        self._next_cursor_lock.acquire()
        try:
            cursor_id = QueryCursor._next_cursor
            QueryCursor._next_cursor += 1
        finally:
            self._next_cursor_lock.release()
        return cursor_id

    @staticmethod
    def _ValidateQuery(query, query_info):
        """Ensure that the given query matches the query_info.

        Args:
            query: datastore_pb.Query instance we are chacking
            query_info: datastore_pb.Query instance we want to match

        Raises BadRequestError on failure.
        """
        error_msg = 'Cursor does not match query: %s'
        exc = datastore_errors.BadRequestError
        if query_info.filter_list() != query.filter_list():
            raise exc(error_msg % 'filters do not match')
        if query_info.order_list() != query.order_list():
            raise exc(error_msg % 'orders do not match')

        for attr in ('ancestor', 'kind', 'name_space', 'search_query'):
            query_info_has_attr = getattr(query_info, 'has_%s' % attr)
            query_info_attr = getattr(query_info, attr)
            query_has_attr = getattr(query, 'has_%s' % attr)
            query_attr = getattr(query, attr)
            if query_info_has_attr():
                if not query_has_attr() or query_info_attr() != query_attr():
                    raise exc(error_msg % ('%s does not match' % attr))
            elif query_has_attr():
                raise exc(error_msg % ('%s does not match' % attr))

    @staticmethod
    def _MinimalQueryInfo(query):
        """Extract the minimal set of information for query matching.

        Args:
            query: A datastore_pb.Query instance from which to extract info.

        Returns:
            The datastore_pb.Query instance suitable for matching against when
            validating cursors.
        """
        query_info = datastore_pb.Query()
        query_info.set_app(query.app())

        for filter in query.filter_list():
            query_info.filter_list().append(filter)
        for order in query.order_list():
            query_info.order_list().append(order)

        if query.has_ancestor():
            query_info.mutable_ancestor().CopyFrom(query.ancestor())

        for attr in ('kind', 'name_space', 'search_query'):
            query_has_attr = getattr(query, 'has_%s' % attr)
            query_attr = getattr(query, attr)
            query_info_set_attr = getattr(query_info, 'set_%s' % attr)
            if query_has_attr():
                query_info_set_attr(query_attr())

        return query_info

    @classmethod
    def _DecodeCompiledCursor(cls, query, compiled_cursor):
        """Convert a compiled_cursor into a cursor_entity.

        Returns:
            (cursor_entity, inclusive): A datastore.Entity and if it should be
            included in the result set.
        """
        assert len(compiled_cursor.position_list()) == 1

        position = compiled_cursor.position(0)
        entity_pb = datastore_pb.EntityProto()
        (query_info_encoded, entity_encoded) = position.start_key().split(
                _CURSOR_CONCAT_STR, 1)
        query_info_pb = datastore_pb.Query()
        query_info_pb.ParseFromString(query_info_encoded)
        if query:
            cls._ValidateQuery(query, query_info_pb)

        entity_pb.ParseFromString(entity_encoded)
        return (datastore.Entity._FromPb(entity_pb, True),
                        position.start_inclusive())

    def _EncodeCompiledCursor(self, query, compiled_cursor):
        """Convert the current state of the cursor into a compiled_cursor.

        Args:
            query: The datastore_pb.Query this cursor is related to.
            compiled_cursor: An empty datstore_pb.CompiledCursor.
        """
        if self.__last_result:
            position = compiled_cursor.add_position()
            query_info = self._MinimalQueryInfo(query)
            start_key = _CURSOR_CONCAT_STR.join((
                    query_info.Encode(),
                    self.__last_result.Encode()))
            position.set_start_key(str(start_key))
            position.set_start_inclusive(False)

    def PopulateQueryResult(self, result, count, compile=False):
        """Populates a QueryResult with this cursor and the given results.

        Args:
            result: A datastore_pb.QueryResult entity.
            count: An integer of how many results to return.
            compile: Boolean, whether we are compiling this query.
        """
        if count > _MAXIMUM_RESULTS:
            count = _MAXIMUM_RESULTS

        result.mutable_cursor().set_app(self.app)
        result.mutable_cursor().set_cursor(self.cursor)
        result.set_keys_only(self.keys_only)

        results = self.__results[self.__offset:self.__offset + count]
        count = len(results)
        if count:
            self.__offset += count
            self.__last_result = results[count - 1]

        results_pbs = results
        result.result_list().extend(results_pbs)

        result.set_more_results(self.__offset < self.count)
        if compile:
            self._EncodeCompiledCursor(
                self.__query, result.mutable_compiled_cursor())


class DatastoreRedisStub(apiproxy_stub.APIProxyStub):
    """Persistent stub for the Python datastore API.

    Uses Redis as backend.
    """

    def __init__(self,
                 app_id,
                 indexes,
                 host='localhost',
                 port=6379,
                 service_name='datastore_v3'):
        """Constructor.

        Initializes the datastore.

        Args:
            app_id: String.
            indexes: List of index definitions.
            host: The Redis host.
            port: The Redis port.
            service_name: Service name expected for all calls.
        """
        super(DatastoreRedisStub, self).__init__(service_name)

        assert isinstance(app_id, basestring) and app_id != ''

        self.__app_id = app_id

        self.__indexes = self._GetIndexDefinitions(indexes)

        # The redis database where we store encoded entity protobufs and our
        # indices.
        self.__db = redis.Redis(host=host, port=port, db=1)
        try:
            self.__db.ping()
        except redis.ConnectionError:
            raise apiproxy_errors.ApplicationError(
                datastore_pb.Error.INTERNAL_ERROR,
                'Redis on %s:%i not available' % (host, port))

        # In-memory entity cache.
        self.__entities_cache = {}
        self.__entities_cache_lock = threading.Lock()
        self.__cursors = {}

        # Sequential IDs.
        self.__next_id_key = _NEXT_ID % {'app': self.__app_id}
        self.__next_id = int(self.__db.get(self.__next_id_key) or 0)
        self.__id_lock = threading.Lock()

        # Transaction set, snapshot and handles.
        self.__transactions = {}
        self.__inside_tx = False
        self.__tx_lock = threading.Lock()
        self.__tx_actions = []
        self.__next_tx_handle = 1
        self.__tx_handle_lock = threading.Lock()

    def Clear(self):
        """Clears all Redis databases of the current application."""

        keys = self.__db.keys('%s!*' % self.__app_id)
        pipe = self.__db.pipeline()
        for key in keys:
            pipe = pipe.delete(key)
        pipe.execute()

        self.__next_id = 1
        self.__transactions = {}
        self.__inside_tx = False
        self.__tx_actions = []
        self.__next_tx_handle = 1
        self.__cursors = {}
        self.__entities_cache = {}

    def __ValidateAppId(self, app_id):
        """Verify that this is the stub for app_id.

        Args:
            app_id: An application ID.

        Raises:
            BadRequestError: if this is not the stub for app_id.
        """
        assert app_id
        if app_id != self.__app_id:
            raise datastore_errors.BadRequestError(
                'app %s cannot access app %s\'s data' % (self.__app_id, app_id))

    def __ValidateKey(self, key):
        """Validate this key.

        Args:
            key: A Reference.

        Raises:
            BadRequestError: if the key is invalid.
        """
        assert isinstance(key, entity_pb.Reference)

        self.__ValidateAppId(key.app())

        for elem in key.path().element_list():
            if elem.has_id() == elem.has_name():
                raise datastore_errors.BadRequestError(
                    'each key path element should have id or name but not '
                    'both: %r' % key)

    def __ValidateTransaction(self, tx):
        """Verify that this transaction exists and is valid.

        Args:
            tx: datastore_pb.Transaction

        Raises:
            BadRequestError: if the tx is valid or doesn't exist.
        """
        assert isinstance(tx, datastore_pb.Transaction)
        self.__ValidateAppId(tx.app())
        if tx not in self.__transactions:
            raise apiproxy_errors.ApplicationError(
                datastore_pb.Error.BAD_REQUEST, 'Transaction %s not found' % tx)

    def _AcquireLockForEntityGroup(self, entity_group='', timeout=_MAX_TIMEOUT):
        """Acquire a lock for a specified entity group.

        The following algorithm helps to avoid a race condition while
        acquireing a lock.

        We assume 3 clients C1, C2 and C3 where C1 is crashed due to an
        uncaught exception.

        - C2 sends SETNX <lock key> in order to acquire the lock.
        - The crashed C1 client still holds it, so Redis will reply with 0
          to C2.
        - C2 GET <lock key> to check if the lock expired. If not it will sleep
          a tenth second and retry from the start.
        - If instead the lock is expired because the UNIX time at <lock key> is
          older than the current UNIX time, C2 tries to perform GETSET
          <lock key> <current unix timestamp + lock timeout + 1>
        - Thanks to the GETSET command semantic C2 can check if the old value
          stored at key is still an expired timestamp. If so we acquired the
          lock!
        - Otherwise if another client, for instance C3, was faster than C2 and
          acquired the lock with the GETSET operation, C2 GETSET operation will
          return a non expired timestamp. C2 will simply restart from the first
          step. Note that even if C2 set the key a bit a few seconds in the
          future this is not a problem.

        (Taken from http://code.google.com/p/redis/wiki/SetnxCommand)

        Args:
            entity_group: An entity group.
            timeout: Number of seconds till a lock expires.
        """
        lock = _ENTITY_GROUP_LOCK % dict(
            app=self.__app_id, entity_group=entity_group)

        while True:
            if self.__db.setnx(lock, time.time() + timeout + 1):
                break
            expires = float(self.__db.get(lock) or 0)
            now = time.time()
            timestamp = now + timeout + 1
            if expires < now:
                expires = float(self.__db.getset(lock, timestamp) or 0)
                if expires < now:
                    break
            else:
                time.sleep(0.1)

    def _ReleaseLockForEntityGroup(self, entity_group=''):
        """Release transaction lock if present.

        Args:
            entity_group: An entity group.
        """
        lock_info = dict(app=self.__app_id, entity_group=entity_group)
        self.__db.delete(_ENTITY_GROUP_LOCK % lock_info)

    @staticmethod
    def _GetIndexDefinitions(indexes):
        """Returns index definitions.

        Args:
            indexes: A list of entity_pb.CompositeIndex instances.

        Returns:
            A dictionary with kinds as keys and entity_pb.Index instances as
            their values.
        """

        return dict(
            [(i.definition().entity_type(), i.definition()) for i in indexes])

    @staticmethod
    def _GetAppIdNamespaceKindForKey(key):
        """Get encoded app and kind from given key.

        Args:
            key: A Reference.

        Returns:
            Encoded app and kind.
        """
        app = datastore_types.EncodeAppIdNamespace(key.app(), key.name_space())

        return '\x08'.join((app, key.path().element_list()[-1].type()))

    @staticmethod
    def _GetRedisKeyForKey(key):
        """Return a unique key.

        Args:
            key: A Reference.

        Returns:
            A key suitable for Redis.
        """
        path = []
        def add_elem_to_path(elem):
            e = elem.type()
            if elem.has_name():
                e += '\x08' + elem.name()
            else:
                e += '\x08\t' + str(elem.id()).zfill(13)
            path.append(e)
        map(add_elem_to_path, key.path().element_list())
        return "%s!%s" % (key.app(), _PATH_CONCAT_STR.join(path))

    def _GetKeyForRedisKey(self, key):
        """Return a unique key.

        Args:
            key: A Redis key.

        Returns:
            A datastore_types.Key instance.
        """
        path = key[len(self.__app_id)+1:].split(_PATH_CONCAT_STR)
        items = []

        for elem in path:
            items.extend(elem.split('\x08'))

        def from_db(value):
            if value.startswith('\t'):
                return int(value[1:])
            return value

        return datastore_types.Key.from_path(*[from_db(a) for a in items])

    def _MakeKeyOnlyEntityForRedisKey(self, key):
        """Make a key only entity.

        Args:
            key: String representing a Redis key.

        Returns:
            An entity_pb.EntityProto instance.
        """

        ref = self._GetKeyForRedisKey(key)._ToPb()

        entity = entity_pb.EntityProto()

        mutable_key = entity.mutable_key()
        mutable_key.CopyFrom(ref)

        mutable_entity_group = entity.mutable_entity_group()
        mutable_entity_group.CopyFrom(ref.path())

        return entity

    def _StoreEntity(self, entity):
        """Store the given entity.

        Args:
            entity: An EntityProto.
        """
        key = entity.key()
        app_kind = self._GetAppIdNamespaceKindForKey(key)
        if app_kind not in self.__entities_cache:
            self.__entities_cache[app_kind] = {}
        self.__entities_cache[app_kind][key] = _StoredEntity(entity)

    @classmethod
    def _GetRedisValueForValue(cls, value):
        """Convert given value.

        Args:
            value: A Python value.

        Returns:
            A string representation of the above Python value.
        """

        if isinstance(value, basestring):
            return value

        return str(value)

    @staticmethod
    def _GetPropertyDict(entity):
        """Get property dictionary.

        Args:
            entity: entity_pb.EntityProto instance.

        Returns:
            Dictionary where property names are mapped to values.
        """

        v = datastore_types.FromPropertyPb
        return dict([(p.name(), v(p)) for p in entity.property_list()])

    @classmethod
    def _IndexKey(cls, key, pipe, unindex=False):
        """Write or delete key to/from key index.

        Args:
            key: A reference.
            pipe: The Redis pipe.
            unindex: Boolean whether index or unindex the key.
        """
        app = key.app()
        kind = key.path().element_list()[-1].type()

        stored_key = cls._GetRedisKeyForKey(key)
        string_key = str(datastore_types.Key._FromPb(key))

        digest = hashlib.md5(string_key).hexdigest()

        key_index = _PROPERTY_INDEX % {
            'app': app, 'kind': kind, 'prop': u'__key__', 'encval': digest}

        kindless_key_index = _PROPERTY_INDEX % {
            'app': app, 'kind': '', 'prop': u'__key__', 'encval': digest}

        if unindex:
            pipe = pipe.srem(key_index, stored_key)
            pipe = pipe.srem(kindless_key_index, stored_key)
        else:
            pipe = pipe.sadd(key_index, stored_key)
            pipe = pipe.sadd(kindless_key_index, stored_key)

        key_prop = _PROPERTY_VALUE % {'key': stored_key, 'prop': '__key__'}

        if unindex:
            pipe = pipe.delete(key_prop)
        else:
            pipe = pipe.set(key_prop, string_key)

        return pipe

    def _IndexEntity(self, entity):
        """Index a given entity.

        Args:
            entity: A _StoredEntity instance.
        """
        assert type(entity) == _StoredEntity

        key = entity.protobuf.key()
        app = key.app()
        kind = key.path().element_list()[-1].type()

        self.__ValidateAppId(app)

        stored_key = self._GetRedisKeyForKey(key)

        pipe = self.__db.pipeline()

        kind_index = _KIND_INDEX % {'app': app, 'kind': kind}
        pipe = pipe.sadd(kind_index, stored_key)

        pipe = self._IndexKey(key, pipe)

        prop_dict = self._GetPropertyDict(entity.protobuf)

        buffers = []

        def _write_key_index(pipe, key_info, value):
            value = self._GetRedisValueForValue(value)
            digest = hashlib.md5(value.encode('utf-8')).hexdigest()
            key_info['encval'] = digest

            # Property index
            prop_index = _PROPERTY_INDEX % key_info
            pipe = pipe.sadd(prop_index, stored_key)

            return pipe

        names = set(entity.native.keys()).difference(
            entity.native.unindexed_properties())

        for name in names:
            value = entity.native[name]
            key_info = dict(app=app, kind=kind, prop=name)
            if isinstance(value, list):
                for item in value:
                    pipe = _write_key_index(pipe, key_info, item)
            else:
                pipe = _write_key_index(pipe, key_info, value)

            # Property types
            value_type = type(prop_dict[name]).__name__
            prop_types = _PROPERTY_TYPES % key_info
            try:
                f = _PROPERTY_VALUE_TYPES.index(value_type)
            except ValueError:
                f = sys.maxint
            pipe = pipe.zadd(prop_types, value_type, f)

            # Property values
            prop_key = _PROPERTY_VALUE % {'key': stored_key, 'prop': name}
            pipe = pipe.set(prop_key, value)

            if isinstance(value, basestring):
                index = indexes.StringIndex(
                    self.__db, self.__app_id, kind, name)
                pipe = index.add(stored_key, value, pipe)

        pipe.execute()

    def _UnindexEntityForKey(self, key):
        """Unindex an entity.

        Args:
            key: An entity_pb.Reference instance.
        """
        app = key.app()
        self.__ValidateAppId(app)

        kind = key.path().element_list()[-1].type()

        stored_key = self._GetRedisKeyForKey(key)
        entity_data = self.__db.get(stored_key)

        if not entity_data:
            return

        entity_proto = entity_pb.EntityProto()
        entity_proto.ParseFromString(entity_data)
        entity = _StoredEntity(entity_proto)

        pipe = self.__db.pipeline()

        kind_index = _KIND_INDEX % {'app': app, 'kind': kind}
        pipe = pipe.srem(kind_index, stored_key)

        pipe = self._IndexKey(key, pipe, unindex=True)

        prop_dict = self._GetPropertyDict(entity.protobuf)

        buffers = []

        names = set(entity.native.keys()).difference(
            entity.native.unindexed_properties())

        for name in names:
            value = self._GetRedisValueForValue(entity.native[name])
            digest = hashlib.md5(value.encode('utf-8')).hexdigest()

            key_info = dict(app=app, kind=kind, prop=name, encval=digest)

            # Property index
            prop_index = _PROPERTY_INDEX % key_info
            pipe = pipe.srem(prop_index, stored_key)

            # Property values
            prop_key = _PROPERTY_VALUE % {'key': stored_key, 'prop': name}
            pipe = pipe.delete(prop_key, value)

            if isinstance(value, basestring):
                index = indexes.StringIndex(
                    self.__db, self.__app_id, kind, name)
                pipe = index.remove(stored_key, value, pipe)

        pipe.execute()

    @staticmethod
    def _GetValueForRedisValue(value, return_type):
        """Convert a Redis value to the desired type.

        Args:
            value: String object.
            return_type: The return value's type.

        Returns:
            A Python value of the desired return type.
        """
        if return_type is datetime:
            new_val = value
        else:
            new_val = return_type(value)

        return new_val

    @classmethod
    def _ApplyOperator(cls, prop, op, term, keys, values):
        """Apply a given operator in combination with a search term.

        Args:
            prop: The property name.
            op: A string representing an equality or inequality operator.
            term: A string or unicode string containing the search term.
            keys: List of Redis keys.
            values: List of Datastore values.
    
        Returns:
            A result list of Redis keys.
        """

        if isinstance(term, datetime):
            term = term.isoformat()

        def _cast(val):
            if not isinstance(term, basestring) and isinstance(val, basestring):
                try:
                    return cls._GetValueForRedisValue(val, type(term))
                except ValueError:
                    return eval(val)
            return val

        values = [_cast(v) for v in values]

        def _flatten(t):
            r = []
            for k, v in t:
                if isinstance(v, list):
                    r.extend([(k, i) for i in v])
                else:
                    r.append((k, v))
            return r

        if prop == '__key__':
            data = sorted(
                _flatten([(keys[p], values[p]) for p in range(len(keys))]),
                key=lambda t:t[0])
        else:
            data = sorted(
                _flatten([(keys[p], values[p]) for p in range(len(keys))]),
                key=lambda t:t[1])

        keys = []; values = []
        for k, v in data:
            keys.append(k)
            values.append(v)

        try:
            i = values.index(term)
            c = values.count(term)
        except ValueError:
            values.append(term)
            values.sort()
            i = values.index(term)
            keys.insert(i, 'nil')
            c = 1

        s = [(keys[p], values[p]) for p in range(len(keys))]

        if op == '<':
            return [p[0] for p in s[0:i]]
        elif op == '>':
            return [p[0] for p in s[i+c:]]
        elif op == '<=':
            return [p[0] for p in s[0:i+c] if p != ('nil', term)]
        elif op == '>=':
            return [p[0] for p in s[i:] if p != ('nil', term)]
        elif op == '==':
            return [p[0] for p in s if p[1] == term]

    @classmethod
    def _ApplyOrderRulesToResults(cls, rules, *results):
        """Apply order rules to query results.

        Args:
            rules: A list containing tuples of order rules
                [('prop_name', direction, [type, type, ...]), ...]

            Each following argument is a pair of two lists representing the
            result of a sort query, where the first member provides Redis keys
            and the second one contains their values, both in the correct order
            regarding above order rules.

        Returns:
            A result list of Redis keys.
        """
        maps = []
        for i in range(0, len(results), 2):
            maps.append((results[i], results[i+1]))
    
        dicts = []
        r = 0
        for m in maps:
            d = dict()
            for i, v in enumerate(m[0]):
                val = m[1][i]
                prop, direction, types = rules[r]
                t = eval(types[0])
                if not isinstance(val, t):
                    val = cls._GetValueForRedisValue(val, t)
                d[v] = val
            r += 1
            dicts.append(d)
    
        tuples = []
        for k in results[0]:
            tuples.append(tuple([k]+[dicts[i][k] for i in range(len(dicts))]))
    
        entities = []
    
        for t in tuples:
            ent = {'key': t[0]}
            for i in range(0, len(rules)):
                prop, direction, types = rules[i]
                ent[prop]=t[i+1]
            entities.append(ent)
    
        def compare_entities(a, b):
            compared = 0
            for rule in rules:
                prop, direction, types = rule
    
                compared = cmp(a[prop], b[prop])
    
                if (direction is 2):
                    compared = -compared
    
                if compared != 0:
                    return compared
    
            if compared == 0:
                return cmp(a['key'], b['key'])
    
        return [e['key'] for e in sorted(entities, compare_entities)]

    def _WriteEntities(self):
        """Write stored entities to Redis backend.

        Uses a Redis Transaction.
        """
        index_entities = []

        for app_kind in self.__entities_cache:
            entities = self.__entities_cache[app_kind]
            for key in entities:
                last_path = key.path().element_list()[-1]
                if last_path.id() != 0 or last_path.has_name():
                    self._UnindexEntityForKey(key)

        # Open a Redis pipeline to perform multiple commands at once.
        pipe = self.__db.pipeline()

        for app_kind in self.__entities_cache:
            entities = self.__entities_cache[app_kind]
            for key in entities:
                entity = entities[key]
                stored_key = self._GetRedisKeyForKey(key)
                pipe = pipe.set(stored_key, entity.encoded_protobuf)
                index_entities.append(entity)

        # Only index successfully written entities.
        if all(pipe.execute()):
            for entity in index_entities:
                self._IndexEntity(entity)

            # Flush our entities cache.
            self.__entities_cache_lock.acquire()
            self.__entities_cache = {}
            self.__entities_cache_lock.release()

    def MakeSyncCall(self, service, call, request, response):
        """The main RPC entry point. service must be 'datastore_v3'."""

        self.assertPbIsInitialized(request)

        if call in ('Put', 'Get', 'Delete'):
            if call == 'Put':
                keys = [e.key() for e in request.entity_list()]
            elif call in ('Get', 'Delete'):
                keys = request.key_list()
            entity_group = self._ExtractEntityGroupFromKeys(keys)
            if request.has_transaction():
                if (request.transaction() in self.__transactions
                        and not self.__inside_tx):
                    self.__inside_tx = True
                    self.__transactions[request.transaction()] = entity_group
                    self._AcquireLockForEntityGroup(entity_group)

        super(DatastoreRedisStub, self).MakeSyncCall(
            service, call, request, response)

        if call in ('Put', 'Delete'):
            if not request.has_transaction():
                self._ReleaseLockForEntityGroup(entity_group)

        if call == 'Commit':
            self._ReleaseLockForEntityGroup(self.__transactions[request])
            del self.__transactions[request]
            self.__inside_tx = False
            self.__tx_lock.release()

        self.assertPbIsInitialized(response)

    @staticmethod
    def assertPbIsInitialized(pb):
        """Raises an exception if the given PB is not initialized and valid."""

        explanation = []
        assert pb.IsInitialized(explanation), explanation
        pb.Encode()

    @staticmethod
    def _ExtractEntityGroupFromKeys(keys):
        """Extracts entity group."""

        types = set([k.path().element_list()[0].type() for k in keys])
        assert len(types) == 1

        return types.pop()

    def _Dynamic_Put(self, put_request, put_response):
        """Implementation of datastore.Put().

        Args:
            put_request: datastore_pb.PutRequest.
            put_response: datastore_pb.PutResponse.
        """
        if put_request.has_transaction():
            self.__ValidateTransaction(put_request.transaction())

        clones = []
        for entity in put_request.entity_list():
            self.__ValidateKey(entity.key())

            clone = entity_pb.EntityProto()
            clone.CopyFrom(entity)

            for property in clone.property_list() + clone.raw_property_list():
                if property.value().has_uservalue():
                    uid = hashlib.md5(
                        property.value().uservalue().email().lower()).digest()
                    uid = '1' + ''.join(['%02d' % ord(x) for x in uid])[:20]
                    mutable_value = property.mutable_value()
                    mutable_value.mutable_uservalue().set_obfuscated_gaiaid(uid)

            clones.append(clone)

            assert clone.has_key()
            assert clone.key().path().element_size() > 0

            last_path = clone.key().path().element_list()[-1]
            if last_path.id() == 0 and not last_path.has_name():
                self.__id_lock.acquire()
                pipe = self.__db.pipeline()
                pipe.incr(self.__next_id_key)
                next_id = int(pipe.execute().pop() or 0)
                self.__next_id = next_id
                last_path.set_id(self.__next_id)
                self.__id_lock.release()

                assert clone.entity_group().element_size() == 0
                group = clone.mutable_entity_group()
                root = clone.key().path().element(0)
                group.add_element().CopyFrom(root)
            else:
                assert (clone.has_entity_group() and
                        clone.entity_group().element_size() > 0)

        self.__entities_cache_lock.acquire()
        try:
            for clone in clones:
                self._StoreEntity(clone)
        finally:
            self.__entities_cache_lock.release()

        if not put_request.has_transaction():
            self._WriteEntities()

        put_response.key_list().extend([c.key() for c in clones])

    def _Dynamic_Get(self, get_request, get_response):
        """Implementation of datastore.Get().

        Args:
            get_request: datastore_pb.GetRequest.
            get_response: datastore_pb.GetResponse.
        """

        if get_request.has_transaction():
            self.__ValidateTransaction(get_request.transaction())

        for key in get_request.key_list():
            self.__ValidateAppId(key.app())

            group = get_response.add_entity()
            data = self.__db.get(self._GetRedisKeyForKey(key))

            if data is None:
                continue

            entity = entity_pb.EntityProto()
            entity.ParseFromString(data)
            group.mutable_entity().CopyFrom(entity)

    def _Dynamic_Delete(self, delete_request, delete_response):
        """Implementation of datastore.Delete().

        Args:
            delete_request: datastore_pb.DeleteRequest.
            delete_response: datastore_pb.DeleteResponse.
        """

        if delete_request.has_transaction():
            self.__ValidateTransaction(delete_request.transaction())

        for key in delete_request.key_list():
            self._UnindexEntityForKey(key)

        # Open a Redis pipeline to perform multiple commands at once.
        pipe = self.__db.pipeline()

        for key in delete_request.key_list():
            self.__ValidateAppId(key.app())

            stored_key = self._GetRedisKeyForKey(key)

            if delete_request.has_transaction():
                app_kind = self._GetAppIdNamespaceKindForKey(key)
                del self.__entities_cache[app_kind][key]
                continue

            pipe = pipe.delete(stored_key)

        if not all(pipe.execute()):
            return

    def _ApplyCursorToQuery(self, query):
        """Apply compiled cursor to given query.

        Args:
            query: An datastore_pb.Query instance.
        """
        cursor = QueryCursor._DecodeCompiledCursor(
            None, query.compiled_cursor())

        new_filter = query.add_filter()
        new_filter.set_op(3)

        new_prop = new_filter.add_property()
        new_prop.set_name('__key__')
        new_prop.set_multiple(False)

        new_val = new_prop.mutable_value()

        referencevalue = new_val.mutable_referencevalue()
        referencevalue.set_app(query.app())
        referencevalue.set_name_space(query.name_space())

        path_element = referencevalue.add_pathelement()
        path_element.set_type(cursor[0].key().kind())
        path_element.set_id(cursor[0].key().id())

        query.clear_compiled_cursor()

    def _Dynamic_RunQuery(self, query, query_result):
        """Run given query.

        Args:
            query: A datastore_pb.Query.
            query_result: A datastore_pb.QueryResult.
        """

        if query.has_transaction():
            self.__ValidateTransaction(query.transaction())
            if not query.has_ancestor():
                raise apiproxy_errors.ApplicationError(
                    datastore_pb.Error.BAD_REQUEST,
                    'Only ancestor queries are allowed inside transactions.')

        app_id = query.app()
        namespace = query.name_space()
        self.__ValidateAppId(app_id)

        if query.has_offset() and query.offset() > _MAX_QUERY_OFFSET:
            raise apiproxy_errors.ApplicationError(
                datastore_pb.Error.BAD_REQUEST, 'Too big query offset.')

        num_components = len(query.filter_list()) + len(query.order_list())
        if query.has_ancestor():
            num_components += 1
        if num_components > _MAX_QUERY_COMPONENTS:
            raise apiproxy_errors.ApplicationError(
                datastore_pb.Error.BAD_REQUEST,
                ('query is too large. may not have more than %s filters'
                ' + sort orders ancestor total' % _MAX_QUERY_COMPONENTS))

        results = []

        (filters, orders) = datastore_index.Normalize(
            query.filter_list(), query.order_list())

        if query.has_compiled_cursor():
            self._ApplyCursorToQuery(query)

        if query.has_offset():
            offset = query.offset()
        else:
            offset = 0

        if query.has_limit():
            limit = query.limit()
        elif query.has_count():
            limit = query.count()
        else:
            limit = _MAXIMUM_RESULTS

        filter_results = []

        for filt in filters:
            assert filt.op() != datastore_pb.Query_Filter.IN
            assert len(filt.property_list()) == 1

            prop = filt.property(0).name().decode('utf-8')
            val = datastore_types.FromPropertyPb(filt.property(0))
            op = _DATASTORE_OPERATORS[filt.op()]

            if isinstance(val, datastore_types.Key):
                val = str(val)

            digest = hashlib.md5(
                self._GetRedisValueForValue(val).encode('utf-8')).hexdigest()

            if query.has_kind():
                kind = query.kind()
            else:
                kind = ''

            key_info = dict(app=app_id, kind=kind, prop=prop, encval=digest)

            if op == '==':
                index = _PROPERTY_INDEX % key_info
                filter_results.append(
                    self.__db.sort(index, alpha=True, start=offset, num=limit))
                continue

            if isinstance(val, basestring) and prop != '__key__':
                keys = indexes.StringIndex(
                    self.__db, self.__app_id, query.kind(), prop)
                filter_results.append(keys.filter(op, val, limit))
            else:
                # TODO This ends up in potentially very large results the more
                # entities of a kind exist. A possible solution could be to
                # partition the indexes.
                if isinstance(val, basestring):
                    alpha = True
                else:
                    alpha = False

                index = _KIND_INDEX % key_info
                pattern = '*:' + prop

                pipe = self.__db.pipeline()
                pipe = pipe.sort(index, by=pattern, alpha=alpha)
                pipe = pipe.sort(index, by=pattern, get=pattern, alpha=alpha)

                keys, vals = pipe.execute()

                filter_results.append(
                    self._ApplyOperator(prop, op, val, keys, vals))

        key_info = dict(app=app_id, kind=query.kind())

        if filter_results:
            result = set(filter_results[0] or [])
            for i in range(1, len(filter_results)):
                result = result & set(filter_results[i] or [])
        else:
            result = set()

        if not filters:
            if orders:
                desc = orders[0].direction() == 2
            else:
                desc = False
            result = set(
                self.__db.sort(
                    _KIND_INDEX % key_info,
                    alpha=True,
                    desc=desc,
                    start=offset,
                    num=limit)
            )

        if query.has_ancestor() and result:
            ancestor_path = query.ancestor().path().element_list()
            def is_descendant(pb):
                path = pb.key().path().element_list()
                return path[:len(ancestor_path)] == ancestor_path
            results = filter(
                is_descendant,
                [entity_pb.EntityProto(d) for d in self.__db.mget(result)])

        if orders:
            pipe = self.__db.pipeline()
            key_info = dict(app=app_id, kind=query.kind())
            for order in orders:
                key_info['prop'] = order.property()
                pipe = pipe.sort(_PROPERTY_TYPES % key_info)
            types = pipe.execute()

            prop_types = {}
            for i in range(len(orders)):
                prop_types[orders[i].property().decode('utf-8')] = types[i]

        if result and orders:
            buf_key = _TEMPORARY_KEY % {'app': app_id, 'uuid': uuid.uuid4()}

            pipe = self.__db.pipeline()

            for elem in result:
                pipe = pipe.rpush(buf_key, elem)

            for order in orders:
                prop = order.property().decode('utf-8')
                if order.direction() == 2:
                    desc = True
                else:
                    desc = False

                prop_type = prop_types.get(prop)
                if set(prop_type) & _REDIS_SORT_ALPHA_TYPES:
                    alpha = True
                else:
                    alpha = False

                pattern = '*:' + prop
                pipe = pipe.sort(
                    buf_key, by=pattern, desc=desc, alpha=alpha)
                pipe = pipe.sort(
                    buf_key, by=pattern, get=pattern, desc=desc, alpha=alpha)

            pipe = pipe.delete(buf_key)

            status = pipe.execute()
            assert status[-1]
            status.pop()
            rules = [(o.property(), o.direction(), prop_types[o.property()])
                for o in orders]
            result = self._ApplyOrderRulesToResults(
                rules, *status[-(len(orders)*2):])
        elif result and not orders:
            result = sorted(result)

        if result and not results:
            if query.keys_only():
                results = [
                    self._MakeKeyOnlyEntityForRedisKey(key) for key in result]
            else:
                results = [
                    entity_pb.EntityProto(pb) for pb in self.__db.mget(result)]

        cursor = QueryCursor(self.__db, query, results)
        self.__cursors[cursor.cursor] = cursor

        cursor.PopulateQueryResult(query_result, limit, compile=query.compile())

        if query.compile():
            compiled_query = query_result.mutable_compiled_query()
            compiled_query.set_keys_only(query.keys_only())
            compiled_query.mutable_primaryscan().set_index_name(query.Encode())

    def _Dynamic_Next(self, next_request, query_result):
        """Gets the next batch of query results.

        Args:
            next_request: A datastore_pb.NextRequest instance.
            query_result: A datastore_pb.QueryResult instance.
        """
        raise NotImplementedError

    def _Dynamic_Count(self, query, integer64proto):
        """Count the number of query results.

        Args:
            query: A datastore_pb.Query instance.
            integer64proto: An api_base_pb.Integer64Proto instance.
        """
        query_result = datastore_pb.QueryResult()
        self._Dynamic_RunQuery(query, query_result)
        integer64proto.set_value(
            min(len(query_result.result_list()), _MAXIMUM_RESULTS))

    def QueryHistory(self):
        """Returns a dict that maps Query PBs to times they've been run."""

        return {}

    def _Dynamic_BeginTransaction(self, request, transaction):
        """Begin a transaction.

        Args:
            request: A datastore_pb.BeginTransactionRequest.
            transaction: A datastore_pb.BeginTransactionRequest instance.
        """
        self.__ValidateAppId(request.app())

        self.__tx_handle_lock.acquire()
        handle = self.__next_tx_handle
        self.__next_tx_handle += 1
        self.__tx_handle_lock.release()

        transaction.set_app(request.app())
        transaction.set_handle(handle)
        assert transaction not in self.__transactions
        self.__transactions[transaction] = None

        self.__tx_actions = []
        self.__tx_lock.acquire()

    def _Dynamic_AddActions(self, request, _):
        """Associates the creation of one or more tasks with a transaction.

        Args:
            request: A taskqueue_service_pb.TaskQueueBulkAddRequest containing
                the tasks that should be created when the transaction is
                comitted.
        """

    def _Dynamic_Commit(self, transaction, response):
        """Commit a transaction.

        Args:
            transaction: A datastore_pb.Transaction instance.
            response: A datastore_pb.CommitResponse instance.
        """
        self.__ValidateTransaction(transaction)

        try:
            self._WriteEntities()

            for action in self.__tx_actions:
                try:
                    apiproxy_stub_map.MakeSyncCall(
                        'taskqueue', 'Add', action, api_base_pb.VoidProto())
                except apiproxy_errors.ApplicationError, e:
                    logging.warning(
                        'Transactional task %s has been dropped, %s', action, e)
                    pass

        finally:
            self.__tx_actions = []

    def _Dynamic_Rollback(self, transaction, unused_response):
        """Perform a rollback of the current transaction.

        Args:
            transaction: A datastore_pb.Transaction instance.
            unused_response: An api_base_pb.VoidProto instance.
        """
        self.__ValidateTransaction(transaction)

        self.__entities_cache = {}
        self.__tx_actions = []

    def _Dynamic_GetSchema(self, req, schema):
        """Find all stored kinds.

        This is used for the development server only.

        Args:
            req: A datastore_pb.GetSchemaRequest instance.
            schema: A datastore_pb.Schema instance.
        """
        app = req.app()
        self.__ValidateAppId(app)

        kind = '*'

        kind_indexes = self.__db.keys(_KIND_INDEX % locals())

        kinds = []

        for key in kind_indexes:
            entities = self.__db.sort(key, alpha=True, start=0, num=1, get='*')
            if entities:
                kinds.append(entity_pb.EntityProto(entities.pop()))

        for kind_pb in kinds:
            kind = schema.add_kind()
            kind.CopyFrom(kind_pb)
            if not req.properties():
                kind.clear_property()

        schema.set_more_results(False)

    def _Dynamic_AllocateIds(self, allocate_ids_request, allocate_ids_response):
        """Allocate a batch of IDs in the datastore.

        Args:
            allocate_ids_request: A datastore_pb.AllocateIdsRequest instance.
            allocate_ids_response: A datastore_pb.AllocateIdsResponse instance.
        """
        model_key = allocate_ids_request.model_key()
        size = allocate_ids_request.size()

        self.__ValidateAppId(model_key.app())

        try:
            self.__id_lock.acquire()
            start = self.__next_id
            pipe = self.__db.pipeline()
            pipe.incr(self.__next_id_key, size)
            next_id = int(pipe.execute().pop() or 0)
            self.__next_id = next_id
            end = self.__next_id - 1
        finally:
            self.__id_lock.release()

        allocate_ids_response.set_start(start)
        allocate_ids_response.set_end(end)

    def _Dynamic_CreateIndex(self, index, id_response):
        """ """

    def _Dynamic_GetIndices(self, app_str, composite_indices):
        """ """

    def _Dynamic_UpdateIndex(self, index, void):
        """ """

    def _Dynamic_DeleteIndex(self, index, void):
        """ """

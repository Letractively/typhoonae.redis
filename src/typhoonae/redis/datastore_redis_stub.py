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

from google.appengine.api import apiproxy_stub
from google.appengine.api import datastore
from google.appengine.api import datastore_errors
from google.appengine.api import datastore_types
from google.appengine.datastore import datastore_index
from google.appengine.datastore import datastore_pb
from google.appengine.datastore import entity_pb
from google.appengine.runtime import apiproxy_errors

import hashlib
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
_KIND_INDEX        = '%(app)s!%(kind)s:\vKEYS'
_NEXT_ID           = '%(app)s!\vNEXT_ID'
_PROPERTY_INDEX    = '%(app)s!%(kind)s:%(prop)s:%(encval)s:\vKEYS'
_PROPERTY_ORDER    = '%(app)s!%(kind)s:%(prop)s:\vORDER'
_PROPERTY_TYPES    = '%(app)s!%(kind)s:%(prop)s:\vTYPES'
_PROPERTY_VALUE    = '%(key)s:%(prop)s'


class _StoredEntity(object):
    """Entity wrapper.

    Provides three variants of the same entity for various stub operations.
    """

    def __init__(self, entity):
      """Constructor.

      Args:
          entity: entity_pb.EntityProto to store.
      """
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

        # Sequential IDs.
        self.__next_id_key = _NEXT_ID % {'app': self.__app_id}
        self.__next_id = int(self.__db.get(self.__next_id_key) or 0)
        if self.__next_id == 0:
            self.__next_id += 1
            self.__db.incr(self.__next_id_key)
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
                e += '\x08\t' + str(elem.id()).zfill(10)
            path.append(e)
        map(add_elem_to_path, key.path().element_list())
        return "%s!%s" % (key.app(), "/".join(path))

    def _GetKeyForRedisKey(self, key):
        """Return a unique key.

        Args:
            key: A Redis key.

        Returns:
            A datastore_types.Key instance.
        """
        path = key[len(self.__app_id)+1:].split('/')
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

    @staticmethod
    def _CalculateScoreForString(data):
        """Get a calculated score for a given string.

        Args:
            data: String data.
        """
        w = ''
        for c in data:
            sw = string.zfill(ord(c), 3)
            w += sw

        # 3 * lenght of max sort depth
        w = w+((3*32)-len(w))*'0'
        return long(w)

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

        index_def = self.__indexes.get(kind)
        if not index_def:
            pipe.execute()
            return

        prop_dict = self._GetPropertyDict(entity.protobuf)

        buffers = []

        for prop in index_def.property_list():
            name = prop.name()
            value = self._GetRedisValueForValue(entity.native[name])
            digest = hashlib.md5(value.encode('utf-8')).hexdigest()

            key_info = dict(app=app, kind=kind, prop=name, encval=digest)

            # Property index
            prop_index = _PROPERTY_INDEX % key_info
            pipe = pipe.sadd(prop_index, stored_key)

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

            # Ordered values (buffers)
            if value_type in ('int', 'float'):
                f = float(value)
            elif value_type in ('str', 'unicode'):
                f = self._CalculateScoreForString(value)
            else:
                f = 0

            prop_order = _PROPERTY_ORDER % {
                'app': app, 'kind': kind, 'prop': name}
            pipe = pipe.zadd(prop_order, stored_key, f)

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

        index_def = self.__indexes.get(kind)
        if not index_def:
            pipe.execute()
            return

        prop_dict = self._GetPropertyDict(entity.protobuf)

        buffers = []

        for prop in index_def.property_list():
            name = prop.name()
            value = self._GetRedisValueForValue(entity.native[name])
            digest = hashlib.md5(value.encode('utf-8')).hexdigest()

            key_info = dict(app=app, kind=kind, prop=name, encval=digest)

            # Property index
            prop_index = _PROPERTY_INDEX % key_info
            pipe = pipe.srem(prop_index, stored_key)

            # Property values
            prop_key = _PROPERTY_VALUE % {'key': stored_key, 'prop': name}
            pipe = pipe.delete(prop_key, value)

            # Ordered values (buffers)
            prop_order = _PROPERTY_ORDER % {
                'app': app, 'kind': kind, 'prop': name}
            pipe = pipe.zrem(prop_order, stored_key)

        pipe.execute()

    def _WriteEntities(self):
        """Write stored entities to Redis backend.

        Uses a Redis Transaction.
        """
        new_ids = 0
        for app_kind in self.__entities_cache:
            for key in self.__entities_cache[app_kind]:
                last_path = key.path().element_list()[-1]
                if last_path.id() == 0 and not last_path.has_name():
                    new_ids += 1

        if new_ids:
            # Allocate integer ID range.
            self.__id_lock.acquire()
            max_id = int(self.__db.incr(self.__next_id_key, new_ids))
            self.__next_id = max_id - new_ids
            self.__id_lock.release()

        index_entities = []

        for app_kind in self.__entities_cache:
            entities = self.__entities_cache[app_kind]
            for key in entities:
                if last_path.id() != 0 or last_path.has_name():
                    self._UnindexEntityForKey(key)

        # Open a Redis pipeline to perform multiple commands at once.
        pipe = self.__db.pipeline()

        for app_kind in self.__entities_cache:
            entities = self.__entities_cache[app_kind]
            for key in entities:

                entity = entities[key]

                last_path = key.path().element_list()[-1]
                if last_path.id() == 0 and not last_path.has_name():
                    # Update sequential integer ID.
                    self.__id_lock.acquire()
                    last_path.set_id(self.__next_id)
                    self.__next_id += 1
                    self.__id_lock.release()

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

        if call in ('Put', 'Delete'):
            if call == 'Put':
                keys = [e.key() for e in request.entity_list()]
            elif call == 'Delete':
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
                del self.__entities_cache[key.app()][key]
                continue

            pipe = pipe.delete(stored_key)

        if not all(pipe.execute()):
            return

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

        (filters, orders) = datastore_index.Normalize(
            query.filter_list(), query.order_list())

        result = []

        pipe = self.__db.pipeline()

        if not filters:
            key_info = dict(app=app_id, kind=query.kind())
            pipe = pipe.sort(_KIND_INDEX % key_info)

        for filt in filters:
            assert filt.op() != datastore_pb.Query_Filter.IN
            assert len(filt.property_list()) == 1

            prop = filt.property(0).name().decode('utf-8')
            val = datastore_types.FromPropertyPb(filt.property(0))
            op = _DATASTORE_OPERATORS[filt.op()]

            digest = hashlib.md5(
                self._GetRedisValueForValue(val).encode('utf-8')).hexdigest()
            key_info = dict(
                app=app_id, kind=query.kind(), prop=prop, encval=digest)

            # TODO Inequality filter implementation needs to be refactored.
            if type(val) in (int, long, float):
                score = float(val)
            elif isinstance(val, basestring):
                score = self._CalculateScoreForString(val)
            else:
                score = 0

            prop_order = _PROPERTY_ORDER % key_info

            if op == '<':
                if type(val) in (int, long, float):
                    pipe = pipe.zrangebyscore(prop_order, 0.0, score)
                else:
                    score = long(score-10**90)
                    pipe = pipe.zrangebyscore(prop_order, 0, score)
            elif op == '<=':
                if type(val) in (int, long, float):
                    pipe = pipe.zrangebyscore(prop_order, 0.0, score)
                else:
                    score = long(score)
                    pipe = pipe.zrangebyscore(prop_order, 0, score)
            elif op == '>':
                if type(val) in (int, long, float):
                    pipe = pipe.zrangebyscore(prop_order, score, sys.maxint)
                else:
                    score = long(score+10**90)
                    pipe = pipe.zrangebyscore(
                        prop_order, score, int('1'+''.zfill(95)))
            elif op == '>=':
                if type(val) in (int, long, float):
                    pipe = pipe.zrangebyscore(prop_order, score, sys.maxint)
                else:
                    score = long(score)
                    pipe = pipe.zrangebyscore(
                        prop_order, score, int('1'+''.zfill(95)))
            else:
                pipe = pipe.sort(_PROPERTY_INDEX % key_info)

        values = pipe.execute()
        if values:
            buffer = set(values[0] or [])
            for i in range(1, len(values)):
                buffer = buffer & set(values[i] or [])

        if orders:
            pipe = self.__db.pipeline()
            key_info = dict(app=app_id, kind=query.kind())
            for order in orders:
                key_info['prop'] = order.property()
                pipe = pipe.sort(_PROPERTY_TYPES % key_info)
            types = pipe.execute()
            prop_val_types = {}
            for i in range(len(orders)):
                prop_val_types[order.property()] = types[i]

        if buffer and orders:
            buf_id = uuid.uuid4()

            pipe = self.__db.pipeline()

            for elem in buffer:
                pipe = pipe.rpush(buf_id, elem)

            for order in orders:
                prop = order.property()
                if order.direction() == 2:
                    desc = True
                else:
                    desc = False
                val_type = prop_val_types.get(order.property())
                if set(val_type) & _REDIS_SORT_ALPHA_TYPES:
                    alpha = True
                else:
                    alpha = False
                pipe = pipe.sort(
                    buf_id, by='*:%s' % prop, desc=desc, alpha=alpha)

            pipe = pipe.delete(buf_id)

            status = pipe.execute()
            assert status[-1]

            # TODO Allow more than one orders.
            buffer = status[(len(orders)+1)*-1]

        if query.keys_only():
            result.extend(buffer)
        else:
            if buffer:
                result.extend(self.__db.mget(buffer))

        if result:
            if query.keys_only():
                query_result.result_list().extend(
                    [self._MakeKeyOnlyEntityForRedisKey(key) for key in result])
            else:
                query_result.result_list().extend(
                    [entity_pb.EntityProto(pb) for pb in result])

        # Pupulating the query result.
        query_result.mutable_cursor().set_app(app_id)
        # TODO Query cursors.
        query_result.mutable_cursor().set_cursor(0)
        query_result.set_keys_only(query.keys_only())
        query_result.set_more_results(False)

    def _Dynamic_Next(self, next_request, query_result):
        """ """

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

    def _Dynamic_Rollback(self, transaction, transaction_response):
        """ """

    def _Dynamic_GetSchema(self, req, schema):
        """ """

    def _Dynamic_AllocateIds(self, allocate_ids_request, allocate_ids_response):
        """ """

    def _Dynamic_CreateIndex(self, index, id_response):
        """ """

    def _Dynamic_GetIndices(self, app_str, composite_indices):
        """ """

    def _Dynamic_UpdateIndex(self, index, void):
        """ """

    def _Dynamic_DeleteIndex(self, index, void):
        """ """

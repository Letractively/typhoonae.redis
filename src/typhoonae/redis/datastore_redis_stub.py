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

Unlike the file stub's implementation it is suitable for larger production
data and handles concurrency.
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
import threading


entity_pb.Reference.__hash__      = lambda self: hash(self.Encode())
datastore_pb.Query.__hash__       = lambda self: hash(self.Encode())
datastore_pb.Transaction.__hash__ = lambda self: hash(self.Encode())

# Constants
_MAXIMUM_RESULTS  = 1000
_MAX_QUERY_OFFSET = 1000
_MAX_QUERY_COMPONENTS = 100

_DATASTORE_OPERATORS = {
    datastore_pb.Query_Filter.LESS_THAN:             '<',
    datastore_pb.Query_Filter.LESS_THAN_OR_EQUAL:    '<=',
    datastore_pb.Query_Filter.GREATER_THAN:          '>',
    datastore_pb.Query_Filter.GREATER_THAN_OR_EQUAL: '>=',
    datastore_pb.Query_Filter.EQUAL:                 '==',
}

# Reserved Redis keys and patterns
_NEXT_ID         = '%(app)s!\vNEXT_ID'
_KIND_INDEX      = '%(app)s!%(kind)s:\vKEYS'
_KIND_INDEX_KEYS = '%(app)s!%(kind)s:\vINDEX_KEYS'
_PROPERTY_INDEX  = '%(app)s!%(kind)s:%(prop)s:%(encval)s:\vKEYS'
_PROPERTY_VALUE  = '%(key)s:%(prop)s'


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
        self.__transactions = set()
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
        self.__transactions = set()
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

        return str(value)

    def _IndexEntity(self, entity):
        """Indexes a given entity.

        Args:
            entity: A _StoredEntity instance.
        """
        assert type(entity) == _StoredEntity

        key = entity.protobuf.key()
        app = key.app()
        kind = key.path().element_list()[-1].type()

        self.__ValidateAppId(app)

        kind_indexes = _KIND_INDEX_KEYS % {'app': app, 'kind': kind}
        index_keys = self.__db.sort(kind_indexes) or []

        stored_key = self._GetRedisKeyForKey(key)

        pipe = self.__db.pipeline()

        for index in [k for k in index_keys if k.endswith(':\vKEYS')]:
            pipe = pipe.srem(index, stored_key)

        kind_index = _KIND_INDEX % {'app': app, 'kind': kind}
        pipe = pipe.sadd(kind_index, stored_key)
        pipe = pipe.sadd(kind_indexes, kind_index)

        index_def = self.__indexes.get(kind)
        if not index_def:
            pipe.execute()
            return

        for prop in index_def.property_list():
            name = prop.name()
            value = self._GetRedisValueForValue(entity.native[name])
            digest = hashlib.md5(value).hexdigest()

            prop_index = _PROPERTY_INDEX % {
                'app': app, 'kind': kind, 'prop': name, 'encval': digest}
            pipe = pipe.sadd(prop_index, stored_key)
            pipe = pipe.sadd(kind_indexes, prop_index)

            prop_key = _PROPERTY_VALUE % {'key': stored_key, 'prop': name}
            pipe = pipe.set(prop_key, value)
            pipe = pipe.sadd(kind_indexes, prop_key)

        pipe.execute()

        self._CleanupPropertyIndexes(kind_indexes, index_keys)

    def _CleanupPropertyIndexes(self, kind_indexes_key, index_keys):
        """Remove deleted property indexes from kind indexes set.

        Args:
            kind_indexes_key: Redis key which holds keys of indexes for a kind.
            index_keys: Redis keys for indexes to remove if empty.

        Returns:
            Boolean whether clening property indexes succeeded.
        """
        pipe = self.__db.pipeline()

        for index in index_keys:
            if index.endswith(':\vKEYS'):
                pipe = pipe.exists(index)

        indexes_to_remove = zip(index_keys, pipe.execute())

        pipe = self.__db.pipeline()

        for index, exists in indexes_to_remove:
            if not exists:
                pipe = pipe.srem(kind_indexes_key, index)

        return False not in pipe.execute()

    def _WriteEntities(self):
        """Write stored entities to Redis backend.

        Uses a Redis Transaction.
        """

        allocate_ids = 0
        for app_kind in self.__entities_cache:
            for key in self.__entities_cache[app_kind]:
                last_path = key.path().element_list()[-1]
                if last_path.id() == 0 and not last_path.has_name():
                    allocate_ids += 1

        if allocate_ids:
            # Allocate integer ID range.
            self.__id_lock.acquire()
            reserved_id = int(
                self.__db.incr(self.__next_id_key, allocate_ids))
            self.__next_id = reserved_id - allocate_ids
            self.__id_lock.release()

        index_entities = []

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
        super(DatastoreRedisStub, self).MakeSyncCall(
            service, call, request, response)
        self.assertPbIsInitialized(response)

    @staticmethod
    def assertPbIsInitialized(pb):
        """Raises an exception if the given PB is not initialized and valid."""

        explanation = []
        assert pb.IsInitialized(explanation), explanation
        pb.Encode()

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

        # Open a Redis pipeline to perform multiple commands at once.
        pipe = self.__db.pipeline()

        for key in delete_request.key_list():
            self.__ValidateAppId(key.app())

            stored_key = self._GetRedisKeyForKey(key)

            if delete_request.has_transaction():
                del self.__entities_cache[key.app()][key]
                continue

            pipe = pipe.delete(stored_key)

        if False in pipe.execute():
            return

        # Update indexes
        for key in delete_request.key_list():
            kind = key.path().element_list()[-1].type()
            stored_key = self._GetRedisKeyForKey(key)

            kind_indexes = _KIND_INDEX_KEYS % {'app': key.app(), 'kind': kind}
            index_keys = self.__db.sort(kind_indexes) or []

            pipe = self.__db.pipeline()

            for index in index_keys:
                if index.startswith(stored_key):
                    pipe = pipe.delete(index)
                    pipe = pipe.srem(kind_indexes, index)
                else:
                    pipe = pipe.srem(index, stored_key)

            pipe.execute()

            self._CleanupPropertyIndexes(kind_indexes, index_keys)

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

        if not filters and not orders:
            pipe = self.__db.pipeline()
            key_info = dict(app=app_id, kind=query.kind())
            pipe = pipe.sort(_KIND_INDEX % key_info, get='*')
            entities_pb = pipe.execute().pop()
            if entities_pb:
                result.extend(entities_pb)

        for filt in filters:
            assert filt.op() != datastore_pb.Query_Filter.IN

            prop = filt.property(0).name().decode('utf-8')
            op = _DATASTORE_OPERATORS[filt.op()]

            property_list = [
                (p.name(), datastore_types.FromPropertyPb(p))
                for p in filt.property_list()]

            pipe = self.__db.pipeline()

            for prop, val in property_list:
                digest = hashlib.md5(
                    self._GetRedisValueForValue(val)).hexdigest()
                key_info = dict(
                    app=app_id, kind=query.kind(), prop=prop, encval=digest)
                pipe = pipe.sort(_PROPERTY_INDEX % key_info, get='*')

            entities_pb = pipe.execute().pop()
            if entities_pb:
                result.extend(entities_pb)

        if result and not query.keys_only():
            query_result.result_list().extend(
                [entity_pb.EntityProto(pb) for pb in result])

        # Pupulating the query result with just nothing for development.
        query_result.mutable_cursor().set_app(app_id)
        query_result.mutable_cursor().set_cursor(0)
        query_result.set_keys_only(query.keys_only())
        query_result.set_more_results(False)

    def _Dynamic_Next(self, next_request, query_result):
        """ """

    def _Dynamic_Count(self, query, integer64proto):
        """ """

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
        self.__transactions.add(transaction)

        self.__tx_actions = []

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
            self.__transactions.remove(transaction)

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

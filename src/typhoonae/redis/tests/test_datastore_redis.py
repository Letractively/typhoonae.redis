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
"""Unit tests for the Datastore Redis stub."""

from google.appengine.api import datastore_types
from google.appengine.datastore import datastore_index
from google.appengine.ext import db

import google.appengine.api.apiproxy_stub
import google.appengine.api.apiproxy_stub_map
import google.appengine.api.datastore_errors
import google.appengine.datastore.entity_pb
import google.appengine.runtime.apiproxy_errors
import os
import time
import threading
import typhoonae.redis.datastore_redis_stub
import unittest

 
class DatastoreRedisTestCase(unittest.TestCase):
    """Testing the TyphoonAE Datastore Redis API proxy stub."""

    def setUp(self):
        """Sets up test environment and regisers stub."""

        # Set required environment variables
        os.environ['APPLICATION_ID'] = 'test'
        os.environ['AUTH_DOMAIN'] = 'mydomain.local'

        # Read index definitions.
        index_yaml = open(
            os.path.join(os.path.dirname(__file__), 'index.yaml'), 'r')

        try:
            indexes = datastore_index.IndexDefinitionsToProtos(
                'test',
                datastore_index.ParseIndexDefinitions(index_yaml).indexes)
        except TypeError:
            indexes = []

        index_yaml.close()

        # Register API proxy stub.
        google.appengine.api.apiproxy_stub_map.apiproxy = (
            google.appengine.api.apiproxy_stub_map.APIProxyStubMap())

        datastore = typhoonae.redis.datastore_redis_stub.DatastoreRedisStub(
            'test', indexes)

        try:
            google.appengine.api.apiproxy_stub_map.apiproxy.RegisterStub(
                'datastore_v3', datastore)
        except google.appengine.runtime.apiproxy_errors.ApplicationError, e:
            raise RuntimeError('These tests require a running Redis server '
                               '(%s)' % e)

        self.stub = google.appengine.api.apiproxy_stub_map.apiproxy.GetStub(
            'datastore_v3')

    def tearDown(self):
        """Clears all data."""

        self.stub.Clear()

    def testStub(self):
        """Tests whether our stub is registered."""

        self.assertNotEqual(None, self.stub)

    def testConnectionError(self):
        """Tries to connect to wrong host and port."""

        self.assertRaises(
            google.appengine.runtime.apiproxy_errors.ApplicationError,
            typhoonae.redis.datastore_redis_stub.DatastoreRedisStub,
            'test', [], host='nowhere', port=10987)

    def test__ValidateAppId(self):
        """Validates an application Id."""

        self.assertRaises(
            google.appengine.api.datastore_errors.BadRequestError,
            self.stub._DatastoreRedisStub__ValidateAppId,
            'foo')

    def test_GetAppIdNamespaceKindForKey(self):
        """Gets encoded app and kind from given key."""

        ref = google.appengine.datastore.entity_pb.Reference()
        ref.set_app(u'test')
        ref.set_name_space(u'namespace')
        path = ref.mutable_path()
        elem = path.add_element()
        elem.set_type('Foo')
        elem = path.add_element()
        elem.set_type('Bar')

        self.assertEqual(
            u'test!namespace\x08Bar',
            self.stub._GetAppIdNamespaceKindForKey(ref))

    def test_GetKeyForRedisKey(self):
        """Inititalizes an entity_pb.Reference from a Redis key."""

        key = self.stub._GetKeyForRedisKey(
            u'test!Foo\x08\t0000000002/Bar\x08bar')
        
        self.assertEqual(
            datastore_types.Key.from_path(
                u'Foo', 2, u'Bar', u'bar', _app=u'test'),
            key)

    def test_GetRedisKeyForKey(self):
        """Creates a valid Redis key."""

        ref = google.appengine.datastore.entity_pb.Reference()
        ref.set_app(u'test')
        ref.set_name_space(u'namespace')
        path = ref.mutable_path()
        elem = path.add_element()
        elem.set_type('Foo')
        elem.set_id(1)
        elem = path.add_element()
        elem.set_type('Bar')
        elem.set_id(2)

        self.assertEqual(
            u'test!Foo\x08\t0000000001/Bar\x08\t0000000002',
            self.stub._GetRedisKeyForKey(ref))

    def testPutGetDelete(self):
        """Puts/gets/deletes entities into/from the datastore."""

        class Author(db.Model):
            name = db.StringProperty()

        class Book(db.Model):
            title = db.StringProperty()

        a = Author(name='Mark Twain', key_name='marktwain')
        a.put()

        b = Book(parent=a, title="The Adventures Of Tom Sawyer")
        b.put()

        key = b.key()
        
        del a, b

        book = google.appengine.api.datastore.Get(key)
        self.assertEqual(
            "{u'title': u'The Adventures Of Tom Sawyer'}", str(book))

        author = google.appengine.api.datastore.Get(book.parent())
        self.assertEqual("{u'name': u'Mark Twain'}", str(author))

        del book

        google.appengine.api.datastore.Delete(key)

        self.assertRaises(
            google.appengine.api.datastore_errors.EntityNotFoundError,
            google.appengine.api.datastore.Get,
            key)

        del author

        mark_twain = Author.get_by_key_name('marktwain')

        self.assertEqual('Author', mark_twain.kind())
        self.assertEqual('Mark Twain', mark_twain.name)

        mark_twain.delete()

    def testLocking(self):
        """Acquires and releases transaction locks."""

        self.stub._AcquireLockForEntityGroup('foo', timeout=1)
        self.stub._ReleaseLockForEntityGroup('foo')

        self.stub._AcquireLockForEntityGroup('bar', timeout=2)
        t = time.time()
        self.stub._AcquireLockForEntityGroup('bar', timeout=1)
        assert time.time() > t + 1
        self.stub._ReleaseLockForEntityGroup('bar')

    def testTransactions(self):
        """Executes 1000 transactions in 10 concurrent threads."""

        class Counter(db.Model):
            value = db.IntegerProperty()

        counter = Counter(key_name='counter', value=0)
        counter.put()

        del counter

        class Incrementer(threading.Thread):
            def run(self):
                def tx():
                    counter = Counter.get_by_key_name('counter')
                    counter.value += 1
                    counter.put()
                for i in range(100):
                    db.run_in_transaction(tx)

        incrementers = []
        for i in range(10):
            incrementers.append(Incrementer())
            incrementers[i].start()

        for incr in incrementers:
            incr.join()

        counter = Counter.get_by_key_name('counter')
        self.assertEqual(1000, counter.value)

    def testRunQuery(self):
        """Runs some simple queries."""

        class Employee(db.Model):
            first_name = db.StringProperty(required=True)
            last_name = db.StringProperty(required=True)
            manager = db.SelfReferenceProperty()

        manager = Employee(first_name='John', last_name='Dowe')
        manager.put()

        employee = Employee(
            first_name=u'John', last_name='Appleseed', manager=manager.key())
        employee.put()

        # Perform a very simple query.
        query = Employee.all()
        self.assertEqual(set(['John Dowe', 'John Appleseed']),
                         set(['%s %s' % (e.first_name, e.last_name)
                              for e in query.run()]))

        # Rename the manager.
        manager.first_name = 'Clara'
        manager.put()

        # And perform the same query as above.
        query = Employee.all()
        self.assertEqual(set(['Clara Dowe', 'John Appleseed']),
                         set(['%s %s' % (e.first_name, e.last_name)
                              for e in query.run()]))

        # Get only one entity.
        query = Employee.all()
        self.assertEqual(u'Dowe', query.get().last_name)
        self.assertEqual(u'Dowe', query.fetch(1)[0].last_name)

        # Delete our entities.
        employee.delete()
        manager.delete()

        # Our query results should now be empty.
        query = Employee.all()
        self.assertEqual([], list(query.run()))

    def testCount(self):
        """Counts query results."""

        class Balloon(db.Model):
            color = db.StringProperty()

        Balloon(color='Red').put()

        self.assertEqual(1, Balloon.all().count())

        Balloon(color='Blue').put()

        self.assertEqual(2, Balloon.all().count())

    def testQueryWithFilter(self):
        """Tries queries with filters."""

        class SomeKind(db.Model):
            value = db.StringProperty()

        foo = SomeKind(value="foo")
        foo.put()

        bar = SomeKind(value="bar")
        bar.put()

        class Artifact(db.Model):
            description = db.StringProperty(required=True)
            age = db.IntegerProperty()

        vase = Artifact(description="Mycenaean stirrup vase", age=3300)
        vase.put()

        helmet = Artifact(description="Spartan full size helmet", age=2400)
        helmet.put()

        unknown = Artifact(description="Some unknown artifact")
        unknown.put()

        query = Artifact.all().filter('age =', 2400)

        self.assertEqual(
            ['Spartan full size helmet'],
            [artifact.description for artifact in query.run()])

        query = db.GqlQuery("SELECT * FROM Artifact WHERE age = :1", 3300)

        self.assertEqual(
            ['Mycenaean stirrup vase'],
            [artifact.description for artifact in query.run()])

        query = Artifact.all().filter('age IN', [2400, 3300])

        self.assertEqual(
            set(['Spartan full size helmet', 'Mycenaean stirrup vase']),
            set([artifact.description for artifact in query.run()]))

        vase.delete()

        query = Artifact.all().filter('age IN', [2400])

        self.assertEqual(
            ['Spartan full size helmet'],
            [artifact.description for artifact in query.run()])

        helmet.age = 2300
        helmet.put()

        query = Artifact.all().filter('age =', 2300)

        self.assertEqual([2300], [artifact.age for artifact in query.run()])

        query = Artifact.all()

        self.assertEqual(
            set([2300L, None]),
            set([artifact.age for artifact in query.run()]))

    def testQueryForKeysOnly(self):
        """Queries for entity keys instead of full entities."""

        class Asset(db.Model):
            name = db.StringProperty(required=True)
            price = db.FloatProperty(required=True)

        lamp = Asset(name="Bedside Lamp", price=10.45)
        lamp.put()

        towel = Asset(name="Large Towel", price=3.50)
        towel.put()

        query = Asset.all(keys_only=True)

        self.assertEqual(
            set([
                datastore_types.Key.from_path(u'Asset', 1, _app=u'test'),
                datastore_types.Key.from_path(u'Asset', 2, _app=u'test')]),
            set(query.run()))

    def testQueryWithOrder(self):
        """Tests queries with sorting."""

        class Planet(db.Model):
            distance = db.FloatProperty()
            name = db.StringProperty()

        earth = Planet(distance=93.0, name="Earth")
        earth.put()

        saturn = Planet(distance=886.7, name="Saturn")
        saturn.put()

        venus = Planet(distance=67.2, name="Venus")
        venus.put()

        mars = Planet(distance=141.6, name="Mars")
        mars.put()

        query = Planet.all().order('distance')

        self.assertEqual(
            ['Venus', 'Earth', 'Mars', 'Saturn'],
            [planet.name for planet in query.run()]
        )

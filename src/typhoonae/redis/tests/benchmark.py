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
"""Benchmarks for the Datastore Redis stub."""

from google.appengine.datastore import datastore_index
from google.appengine.ext import db

import cStringIO
import google.appengine.api.apiproxy_stub
import google.appengine.api.apiproxy_stub_map
import google.appengine.runtime.apiproxy_errors
import os
import random
import sys
import time
import typhoonae.redis.datastore_redis_stub


INDEX_DEFINITIONS = """
indexes:

- kind: FirstModel
  properties:
  - name: prop1
  - name: prop2
  - name: prop3
  - name: prop4
  - name: prop5

- kind: SecondModel
  properties:
  - name: prop1
  - name: prop2
  - name: prop3
  - name: prop4
  - name: prop5
"""


class FirstModel(db.Model):
    prop1 = db.IntegerProperty()
    prop2 = db.StringProperty()
    prop3 = db.IntegerProperty()
    prop4 = db.StringProperty()
    prop5 = db.IntegerProperty()


class SecondModel(db.Model):
    prop1 = db.IntegerProperty()
    prop2 = db.StringProperty()
    prop3 = db.IntegerProperty()
    prop4 = db.StringProperty()
    prop5 = db.IntegerProperty()


def get_datastore_stub():
    # Set required environment variables
    os.environ['APPLICATION_ID'] = 'test'
    os.environ['AUTH_DOMAIN'] = 'mydomain.local'

    # Read index definitions.
    index_yaml = cStringIO.StringIO(INDEX_DEFINITIONS)

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

    return google.appengine.api.apiproxy_stub_map.apiproxy.GetStub(
        'datastore_v3')


def add_random_entities(num, kind):
    r = random.Random()
    numbers = range(1000)
    chars = [' ']+[chr(i) for i in range(65, 104)]
    for n in range(num):
        s = ''.join(r.sample(chars[:40], 40))*2
        i = r.sample(numbers, 1).pop() 

        entity = kind(prop1=i, prop2=s, prop3=i, prop4=s, prop5=i)
        entity.put()


def main():
    try:
        rounds = int(sys.argv[1])
    except IndexError:
        rounds = 1
    stub = get_datastore_stub()
    num = 200

    for kind in (FirstModel, SecondModel):
        for round in range(rounds):
            print "Round %i" % (round+1)
            print "------" + "-" * len('%i' % (round+1))

            sys.stdout.write("Adding %i entities with random data... " % num)
            sys.stdout.flush()
            start = time.time()
            add_random_entities(num, kind)
            end = time.time()
            result = end-start
            if result < 1.0:
                print result * 1000.0, "ms"
            else:
                print result, "sec"

            print num/result, "entities/sec"

            sys.stdout.write("Querying entities ordered by 'prop1'... ")
            query = db.GqlQuery(
                "SELECT * FROM %s ORDER BY prop1" % kind.__name__)
            start = time.time()
            results = list(query.fetch(1000))
            end = time.time()
            assert len(results) > 0 and len(results) <= 1000
            result = end-start
            if result < 1.0:
                print result * 1000.0, "ms"
            else:
                print result, "sec"
            print

    print "Stats"
    print "-----"
    datastore = stub.__dict__['_DatastoreRedisStub__db']
    print "Total number of keys in database:", len(datastore.keys())

    #stub.Clear()

if __name__ == "__main__":
    main()
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
"""Unit tests for Redis Datastore Indexes."""


from typhoonae.redis import indexes

import redis
import unittest


class IndexesTestCase(unittest.TestCase):
    """Testing Redis Datastore Indexes."""

    def setUp(self):
        """Sets up a connection to the Redis test database."""

        self.db = redis.Redis()

        data = [
            'foo', 'bar', 'test', 'foobar', 'test data', 'some string',
            'another string', 'trees are green', 'birds', 'once in a lifetime',
            'road to nowhere\n', 'fools on the hill', 'barny', 'some strings',
            'test drive', 'roadrunner', 'master', 'continue', 'professor',
            'america', 'american dream', u'äquator', 'broadcast', 'dinosaur',
        ]

        index = indexes.StringIndex(self.db, 'app', 'Kind', 'prop')

        id = 0

        for s in data:
            id += 1
            entity_key = 'app!Kind\x08\t'+str(id).zfill(5)
            key = 'app!Kind\x08\t'+str(id).zfill(5)+':prop'
            self.db[key] = s
            index.add(entity_key, s)

    def tearDown(self):
        """Clears all test data."""

        self.db.flushall()

    def testStringIndex(self):
        """Tests the partitioned string index."""

        def sort(results):
            db = self.db
            return [db[k+':prop'] for k
                    in sorted(results, key=lambda v: db.get(v+':prop'))]

        index = indexes.StringIndex(self.db, 'app', 'Kind', 'prop')

        self.assertEqual(
            ['america', 'american dream', 'another string', 'bar', 'barny',
             'birds', 'broadcast', 'continue', 'dinosaur'],
            sort(index.filter('<', 'foo')))

        self.assertEqual(
            ['foobar', 'fools on the hill', 'master', 'once in a lifetime',
             'professor', 'road to nowhere\n', 'roadrunner', 'some string',
             'some strings', 'test', 'test data', 'test drive',
             'trees are green', 'äquator'],
            sort(index.filter('>', 'foo')))

        self.assertEqual(
            [],
            sort(index.filter('>', u'äquator')))

        index.remove('app!Kind\x08\t00009', 'birds')

        self.assertEqual(
            ['america', 'american dream', 'another string', 'bar', 'barny',
             'broadcast', 'continue', 'dinosaur', 'foo', 'foobar',
             'fools on the hill', 'master', 'once in a lifetime', 'professor',
             'road to nowhere\n', 'roadrunner', 'some string', 'some strings',
             'test', 'test data', 'test drive', 'trees are green'],
            sort(index.filter('<', u'äquator')))

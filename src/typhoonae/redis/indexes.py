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
"""Redis Datastore Indexes.

Partitioning addresses key issues in supporting very large indexes by letting
you decompose them into smaller and more manageable pieces called partitions.
Also, partitioning should be entirely transparent to applications.
"""


_SCORES_INDEX   = '%(app)s!%(kind)s:%(prop)s:\vSCORES'
_PROPERTY_SCORE = '%(app)s!%(kind)s:%(prop)s\x08\t'
_PROPERTY_VALUE = '%(key)s:%(prop)s'


class StringIndex(object):
    """Indexing string values."""

    def __init__(self, db, app, kind, prop, depth=2):
        self.__db = db
        self.__prop = prop
        self.__key = _SCORES_INDEX % locals()
        self.__prop_key = _PROPERTY_SCORE % locals()
        self.__depth = depth

    def __score(self, val):
        d = self.__depth
        score = ''.join([str(ord(c)).zfill(3) for c in val[:d]]).ljust(d*3,'0')
        return self.__prop_key+score

    def _execute(self, func, key, value=None, pipe=None):
        assert func in ('sadd', 'srem')
        if not value:
            value = self.__db[key]
        if not pipe:
            _pipe = self.__db.pipeline()
        else:
            _pipe = pipe
        score = self.__score(value)
        _pipe = getattr(_pipe, func)(score, key)
        _pipe = getattr(_pipe, func)(self.__key, score)
        if pipe:
            return pipe
        else:
            return _pipe.execute()

    def add(self, key, value=None, pipe=None):
        return self._execute('sadd', key, value, pipe)

    def remove(self, key, value=None, pipe=None):
        return self._execute('srem', key, value, pipe)

    def _partitions(self, op, score):
        keys = self.__db.sort(self.__key)
        if op in ('<', '<='):
            for p in reversed(filter(lambda k: k<=score, keys)): yield p
        if op in ('>', '>='):
            for p in sorted(filter(lambda k: k>=score, keys)): yield p

    def filter(self, op, value, limit=1000):
        """Apply filter rules.

        Args:
            op: An operator.
            value: A string object.
            limit: The number of results to return.
        """
        score = self.__score(value)
        results = []

        for p in self._partitions(op, score):
            keys = self.__db.sort(p)

            pipe = self.__db.pipeline()
            for k in keys:
                prop_key = _PROPERTY_VALUE % {'key': k, 'prop': self.__prop}
                pipe = pipe.get(prop_key)
            values = pipe.execute()

            buf = [(keys[i], values[i].decode('utf-8'))
                   for i in range(len(keys))]

            if op == '<':
                results.extend([k for k, v in buf if v < value])
            elif op == '<=':
                results.extend([k for k, v in buf if v <= value])
            elif op == '>':
                results.extend([k for k, v in buf if v > value])
            elif op == '>=':
                results.extend([k for k, v in buf if v >= value])

            if len(results) >= limit: break
 
        return results

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


import uuid


_SCORE_INDEX    = '%(app)s!%(kind)s:%(prop)s:\vSCORES'
_PROPERTY_SCORE = '%(app)s!%(kind)s:%(prop)s:\r%(score)s:\vKEYS'
_PROPERTY_VALUE = '%(key)s:%(prop)s'
_TEMPORARY_KEY  = '%(app)s!TEMP:%(uuid)s'


class BaseIndex(object):
    """The base index class."""

    def __init__(self, db, app, kind, prop):
        self.__db = db
        self.__app = app
        self.__kind = kind
        self.__prop = prop
        self.__key = _SCORE_INDEX % locals()

    @property
    def db(self):
        return self.__db

    @property
    def app(self):
        return self.__app

    @property
    def kind(self):
        return self.__kind

    @property
    def prop(self):
        return self.__prop

    @property
    def key(self):
        return self.__key

    def get_score(self, val):
        raise NotImplemented

    def _execute(self, func, key, value=None, pipe=None):
        assert func in ('sadd', 'srem')
        if value is None:
            value = self.db[key]
        if not pipe:
            _pipe = self.db.pipeline()
        else:
            _pipe = pipe
        score = self.get_score(value)
        _pipe = getattr(_pipe, func)(score, key)
        _pipe = getattr(_pipe, func)(self.key, score)
        if pipe:
            return pipe
        else:
            return _pipe.execute()

    def add(self, key, value=None, pipe=None):
        return self._execute('sadd', key, value, pipe)

    def remove(self, key, value=None, pipe=None):
        return self._execute('srem', key, value, pipe)

    def _partitions(self, op, score):
        keys = self.db.sort(self.key)
        if op in ('<', '<='):
            for p in reversed(filter(lambda k: k<=score, keys)): yield p
        if op in ('>', '>='):
            for p in sorted(filter(lambda k: k>=score, keys)): yield p

    def get_value(self, val):
        raise NotImplemented

    def filter(self, op, value, limit=1000, offset=0):
        """Apply filter rules.

        Args:
            op: An operator.
            value: A string object.
            limit: The number of results to return.
            offset: The number of results to skip.
        """
        score = self.get_score(value)
        results = []

        if op == '<':
            cond = (-1,)
            desc = True
        if op == '<=':
            cond = (-1, 0)
            desc = True
        if op == '>':
            cond = (1,)
            desc = False
        if op == '>=':
            cond = (0, 1)
            desc = False

        if isinstance(value, basestring):
            alpha = True
        else:
            alpha = False

        buf_key = _TEMPORARY_KEY % {'app': self.app, 'uuid': uuid.uuid4()}
        for p in self._partitions(op, score):
            pipe = self.db.pipeline()
            for k in self.db.sort(p):
                pipe = pipe.rpush(buf_key, k)
            pipe.execute()

        prop_key = "*:"+self.prop

        all_values = self.db.sort(
            buf_key, by=prop_key, get=prop_key, alpha=alpha, desc=desc)

        if isinstance(value, unicode):
            value = str(value.encode('utf-8'))
        if value not in all_values:
            all_values.append(value)
            all_values.sort(
                lambda a,b:cmp(unicode(a,'utf-8'), unicode(b, 'utf-8')))
            if desc:
                all_values.reverse()

        pos = all_values.index(value)

        keys = self.db.sort(
            buf_key, by=prop_key, alpha=alpha, desc=desc, start=pos+offset,
            num=limit)
        values = self.db.sort(
            buf_key, by=prop_key, get=prop_key, alpha=alpha, desc=desc,
            start=pos+offset, num=limit)

        self.db.delete(buf_key)

        buf = [(keys[i], self.get_value(values[i]))
               for i in range(len(keys))]

        count = 0

        for k, v in buf:
            if cmp(v, value.decode('utf-8')) in cond:
                results.append(k)
                count += 1
            if count >= limit:
                break
 
        return results


class StringIndex(BaseIndex):
    """Indexing string values."""

    def __init__(self, db, app, kind, prop, depth=2):
        super(StringIndex, self).__init__(db, app, kind, prop)
        self.__depth = depth

    def get_score(self, val):
        d = self.__depth
        score = ''.join([str(ord(c)).zfill(5) for c in val[:d]]).ljust(d*5,'0')
        key_info = dict(
            app=self.app, kind=self.kind, prop=self.prop, score=score)
        return _PROPERTY_SCORE % key_info

    def get_value(self, val):
        return val.decode('utf-8')

=========================
TyphoonAE Redis Datastore
=========================

An alternate Datastore backend for App Engine using Redis.

Introduction
============

This package contains an API proxy stub to connect TyphoonAE or the Google App
Engine SDK to Redis as Datastore backend.

Redis is an advanced key-value store. Its dataset is not volatile, and values
can be strings, lists, sets, and ordered sets. All this data types can be
manipulated with atomic operations to push/pop elements, add/remove elements,
perform server side union, intersection, difference between sets, and so forth.
Redis supports different kind of sorting abilities.

See http://code.google.com/p/redis for further information on Redis.


Copyright and license
---------------------

Copyright 2010 Tobias Rodaebel

This software is released under the Apache License, Version 2.0. You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0


Using TyphoonAE Redis Datstore with the Google App Engine SDK
=============================================================

In order to use the Redis Datastore just start the development appserver with
an additional option:

  $ bin/dev_appserver --use_redis parts/google_appengine/demos/guestbook/

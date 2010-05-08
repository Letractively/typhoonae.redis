=========================
TyphoonAE Redis Datastore
=========================

This package contains an API proxy stub to connect TyphoonAE or the Google App
Engine SDK to a Redis database.


Introduction
============

Redis is an advanced key-value store. In contrast to approaches like memcache,
its dataset is not volatile, and values can be strings, lists, sets, and
ordered sets. All this data types can be manipulated with atomic operations to
push/pop elements, add/remove elements, perform server side union,
intersection, difference between sets, and so forth. And Redis supports
different kinds of sorting abilities.

Since Redis provides master-slave replication with very fast non-blocking first
synchronization and auto reconnection on net split, it is very interesting to
be used as Datastore backend for TyphoonAE. This package contains the API proxy
stub to seamlessly connect TyphoonAE or the Google App Engine SDK to a Redis
database server.

See http://code.google.com/p/redis for further information on Redis.


Copyright and License
=====================

Copyright 2010 Tobias Rodaebel

This software is released under the Apache License, Version 2.0. You may obtain
a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0


Building and Testing
====================

Get a local copy of the TyphoonAE Redis repository with this command:

  $ hg clone https://redis.typhoonae.googlecode.com/hg/ typhoonae-redis

Change into the typhoonae-redis directory and run the buildout:

  $ python bootstrap.py
  $ bin/buildout

To run all unit tests start the Redis server and enter the following command:

  $ bin/nosetests


Using the TyphoonAE Redis Datstore with the Google App Engine SDK
=================================================================

This buildout already downloads and patches the Google App Engine SDK for you.
In order to use the Redis Datastore just start the development appserver with
an additional option:

  $ bin/dev_appserver --use_redis parts/google_appengine/demos/guestbook/

A Redis server should be listening on port 6379.


Contributing
============

Since the TyphoonAE project uses Mercurial as SCM, you can easily create a
clone of it on http://code.google.com/p/typhoonae/source/clones?repo=redis.

Visit the project page http://typhoonae.googlecode.com for further information.

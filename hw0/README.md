**DISCLAIMER**

In modern world it is not always easy to say to what type does a certain database belong exactly,
because developers are constanty trying to mitigate all disadvantages of their product.
So for example, a given DB may be of a CA type but also a P type in some sense at the same time.
Also in some cases the type of the same DB can vary with settings that are used in a project.
Considering what is said above the ranking presented below should be viewed as a soft classification
rather than a strict one.

*(There are also some disadvantages of the CAP-theorem itself, which were partially discussed on the*
*lecture, like inaccuracy of terms and so on).*

### [DragonFly](https://github.com/dragonflydb/dragonfly)

**Type:** CP

**Why:** DragonFly developers set their ambitious mission as a creation of
[Redis](https://github.com/redis/redis) replacement like if it was done in 2022.
Redis [belongs](https://stackoverflow.com/questions/59511275/redis-availability-and-cap-theorem)
to CP family (arguably). As for now, it is not that easy to determine the exact type of DrangonFly,
since they are at a beta stage (v0.8.0). But if they compare themselves to Redis and Memcached they are
likely to build the system of the same type.

### [ScyllaDB](https://github.com/scylladb/scylladb)

**Type:** AP (with tunable C, up to immediate Consistency)

**Why:** Scylla's architecture is [clearly](https://www.scylladb.com/product/technology/) inspired by
[Cassandra](https://cassandra.apache.org/_/cassandra-basics.html) which is AP-type DB due to master-master
design. Like Cassandra Scylla also [has](https://www.scylladb.com/product/technology/high-availability/)
eventual consistency by default, meaning the data is considered sufficient right after the moment it is
written on *any* node. But this can be tuned such that consistency is achieved only after a number of nodes
receive update of data. This number of nodes can vary up to all nodes which implies immediate consistency
(real C in terms of CAP-theorem).

### [ArenadataDB](https://github.com/arenadata/gpdb)

**Type:** CA

**Why:** This is a fork of [Greenplum](https://github.com/greenplum-db/gpdb) which is itself based on
[PostgreSQL](https://github.com/postgres/postgres).
The latter is usually [considered](https://habr.com/ru/post/328792/) as CA-type DB
(though it can be configured in a different ways).

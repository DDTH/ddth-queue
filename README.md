ddth-queue
==========

DDTH's libary to interact with various queue implementations.

Project home:
[https://github.com/DDTH/ddth-queue](https://github.com/DDTH/ddth-queue)


## Introduction ##

I work with queues from projects to projects.
However, different projects are fit with different queues.
I need a unified and simple API set to interact with various queue backend systems,
also extra functionalities such as commit queue item when done or re-queue the item if needed,
or find orphan queue items (items that have not been committed for a long period).
Hence this library is born to fulfill my need.


## Installation ##

Latest release version: `0.4.0.1`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency: if only a sub-set of `ddth-queue` functionality is used, choose the
corresponding dependency artifact(s) to reduce the number of unused jar files.

`ddth-queue-core`: ddth-queue interfaces and in-memory (using `java.util.Queue`) implementations:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-queue-core</artifactId>
	<version>0.4.0.1</version>
</dependency>
```

`ddth-queue-disruptor`: include `ddth-queue-core` and [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-disruptor</artifactId>
    <version>0.4.0.1</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-jdbc`: include `ddth-queue-core` and [`ddth-dao-jdbc`](https://github.com/DDTH/ddth-dao) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-jdbc</artifactId>
    <version>0.4.0.1</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-kafka`: include `ddth-queue-core` and [`ddth-kafka`](https://github.com/DDTH/ddth-kafka) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-kafka</artifactId>
    <version>0.4.0.1</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-redis`: include `ddth-queue-core` and [`Jedis`](https://github.com/xetorthio/jedis) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-jedis</artifactId>
    <version>0.4.0.1</version>
    <type>pom</type>
</dependency>
```

## Usage ##

`ddth-queue` provides a unified and simple APIs to interact with various queue implementations:

- Put an item to queue: queue or re-queue.
- Take an item from queue.
- Retrive list of orphan items.


### Queue Usage Flow ###

- Call `IQueue.take()` to take a message from queue.
- Do something with the message.
  - When done, call `IQueue.finish(msg)`
  - If not done and the message need to be re-queued, either call `IQueue.requeue(msg)` or `IQueue.requeueSilent(msg)` to put back the message to queue.

### Queue Storage Implementation ###

Queue implementation has 2 message storages:

- *Queue storage*: (required) main storage where messages are put into and taken from. Queue storage is FIFO.
- *Ephemeral storage*: (optional) messages taken from queue storage are temporarily store in a ephemeral until _finished_ or _re-queued_.

(Queue implementation is required to provide *Queue storage*. *Ephemeral storage* is optional)

When `IQueue.take()` is called, the message is put in a ephemeral storage.
When either `IQueue.finish(msg)` or  `IQueue.requeue(msg)` or `IQueue.requeueSilent(msg)` is called,
the message is removed from the ephemeral storage.

The idea of the ephemeral storage is to make sure messages are not lost in the case the application
crashes in between `IQueue.take()` and `IQueue.finish(msg)` (or `IQueue.requeue(msg)`, or `IQueue.requeueSilent(msg)`).

#### Orphan Messages ####

If the application crashes in between `IQueue.take()` and `IQueue.finish(msg)` (or `IQueue.requeue(msg)`, or `IQueue.requeueSilent(msg)`)
there could be orphan messages left in the ephemeral storage. To deal with orphan messages:

- Call `Collection<IQueueMessage> getOrphanMessages(long thresholdTimestampMs)` to get all orphan messages that were queued _before_ `thresholdTimestampMs`.
- Call `IQueue.finish(msg)` to completely clear the orphan message from the ephemeral storage, or
- Call `IQueue.requeue(msg)`, or `IQueue.requeueSilent(msg)` to move the message back to the queue.


### APIs ###

*`boolean IQueue.queue(IQueueMessage)`*: Put a message to queue storage.

*`boolean requeue(IQueueMessage)`*: Re-queue a taken message. Queue implementation must remove the message instance in the ephemeral storage (if any). Once re-queued, message's timestamp and number of re-queue times are updated.

*`boolean requeueSilent(IQueueMessage)`*: Similar to API `requeue` but message's timestamp and number of re-queue times are _not_ updated.

*`IQueueMessage take()`*: Take a message from queue.

*`Collection<IQueueMessage> getOrphanMessages(long)`*: Gets all orphan messages (messages that were left in ephemeral storage for a long time).

*`finish(IQueueMessage)`*: Called to clean-up message from ephemeral storage.

*`int queueSize()`*: Gets queue's number of items.

*`int ephemeralSize()`*: Gets ephemeral-storage's number of items.


## Built-in Queue Implementations ##

| Implementation | Bounded Size | Persistent | Ephemeral Storage | Multi-Clients |
|----------------|:------------:|:----------:|:-----------------:|:-------------:|
| In-memory      | Optional     | No         | Yes               | No            |
| JDBC           | No           | Yes        | Yes               | Yes           |
| Kafka          | No           | Yes        | No                | Yes           |
| Redis          | No           | Yes (1)    | Yes               | Yes           |
| RocksDB        | No           | Yes        | No                | No            |

- *Bounded Size*: queue's size is bounded.
  - Currently only in-memory queue(s) support bounded queue size
  - Databases, Kafka, Redis and RocksDB are virtually limited only by hardware's capability
- *Persistent*: queue's items are persistent between JVM restarts.
  - Redis backend: persistency depends on Redis server's configurations
- *Ephemeral Storage*: supports retrieval of orphan messages.
- *Multi-Clients*: multi-clients can share a same queue backend storage.


### In-Memory Queue ###

There are 2 implementations:

- [InmemQueue.java](ddth-queue-core/src/main/java/com/github/ddth/queue/impl/InmemQueue.java):
uses a `java.util.Queue` as Queue storage and a `java.util.concurrent.ConcurrentMap` as Ephemeral storage. 
- [DisruptorQueue.java](ddth-queue-core/src/main/java/com/github/ddth/queue/impl/DisruptorQueue.java):
use [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) as Queue storage
and a `java.util.concurrent.ConcurrentMap` as Ephemeral storage.

Messages in in-memory queues are _not_ persistent between JVM restarts!

### JDBC Queue ###

Queue messages are stored in RDBMS tables.

See [JdbcQueue.java](ddth-queue-core/src/main/java/com/github/ddth/queue/impl/JdbcQueue.java).

Queue messages are persistent.

### Kafka Queue ###

This queue implementation utilizes [Apache Kafka](http://kafka.apache.org) as queue storage.

_Ephemeral storage is currently not supported!_

Queue messages are persistent.

See [KafkaQueue.java](ddth-queue-core/src/main/java/com/github/ddth/queue/impl/KafkaQueue.java).

### Redis Queue ###

Queue storage and Ephemeral storage are implemented as a Redis hash and sorted set respectively. 
Also, a Redis list is used as a queue of message-ids.

Queue messages are persistent (depends on Redis server's configurations).

See [RedisQueue.java](ddth-queue-core/src/main/java/com/github/ddth/queue/impl/RedisQueue.java).

### RocksDB Queue ###

Queue messages are stored in [RocskDB](http://rocksdb.org).

_Ephemeral storage is currently not supported!_

Queue messages are persistent.

See [RocksDbQueue.java](ddth-queue-core/src/main/java/com/github/ddth/queue/impl/RocksDbQueue.java).


## Pre-made Convenient Classes ##

- `com.github.ddth.queue.impl.universal.*`: universal queue implementations, where queue's message's id is a `64-bit long`.
- `com.github.ddth.queue.impl.universal2.*`: universal queue implementations, where queue's message's id is a `String`.

### UniversalQueueMessage ###

Universal queue message implementation, with the following fields:

- `queue_id`: message's unique id
  - `com.github.ddth.queue.impl.universal.UniversalQueueMessage`: queue-id is a 64-bit `long`
  - `com.github.ddth.queue.impl.universal2.UniversalQueueMessage`: queue-id is a `String`
- `org_timestamp` (`java.util.Date`): timestamp when the message was first-queued
- `timestamp` (`java.util.Date`): message's last-queued timestamp
- `num_requeues` (`int`): number of times the message has been re-queued
- `content` (`byte[]`): message's content

### UniversalInmemQueue ###

Universal in-memory queue implementation that uses `java.util.Queue` as Queue storage,
and `java.util.concurrent.ConcurrentMap` as Ephemeral storage.

`universal.UniversalInmemQueue` to work with `universal.UniversalQueueMessage`, and
`universal2.UniversalInmemQueue` to work with `universal2.UniversalQueueMessage`.

### UniversalDisruptorQueue ###

Universal in-memory queue implementation that uses [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) as Queue storage,
and `java.util.concurrent.ConcurrentMap` as Ephemeral storage.

`universal.UniversalDisruptorQueue` to work with `universal.UniversalQueueMessage`, and
`universal2.UniversalDisruptorQueue` to work with `universal2.UniversalQueueMessage`.

### UniversalJdbcQueue ###

Universal JDBC queue implementation:

- 2 db tables for queue and ephemeral storages
- `universal.UniversalRedisQueue` to work with `universal.UniversalQueueMessage`, and `universal2.UniversalRedisQueue` to work with `universal2.UniversalQueueMessage`
- Property `ephemeralDisabled` (default `false`): when set to `true` ephemeral storage is disabled
- Property `fifo` (default `true`): when set to `true` messages are taken in FIFO manner

Sample table schema for MySQL: see [sample_schema.mysql.sql](sample-dbschema/sample_schema.mysql.sql).

Sample table schema for PgSQL: see [sample_schema.pgsql.sql](sample-dbschema/sample_schema.pgsql.sql).

### LessLockingUniversalMySQLQueue ###

Similar to `UniversalJdbcQueue`, but using a less-locking algorithm - specific for MySQL, and needs
only one single db table for both queue and ephemeral storages.

- Optimized for MySQL (EXPERIMENTAL!)
- 1 single db table for both queue and ephemeral storages
- Work with `UniversalQueueMessage`
- Property `fifo` (default `true`): when set to `true` messages are taken in FIFO manner

Sample table schema for MySQL: see [sample_schema-less-locking.mysql.sql](sample-dbschema/sample_schema-less-locking.mysql.sql).

### LessLockingUniversalPgSQLQueue ###

Similar to `UniversalJdbcQueue`, but using a less-locking algorithm - specific for PostgreSQL, and needs
only one single db table for both queue and ephemeral storages.

- Optimized for PostgreSQL (EXPERIMENTAL!)
- 1 single db table for both queue and ephemeral storages
- Work with `UniversalQueueMessage`
- Property `fifo` (default `true`): when set to `true` messages are taken in FIFO manner

Sample table schema for MySQL: see [sample_schema-less-locking.pgsql.sql](sample-dbschema/sample_schema-less-locking.pgsql.sql).

### UniversalKafkaQueue ###

Universal queue implementation that uses [Apache Kafka](http://kafka.apache.org) as queue backend.

- Ephemeral storage is currently _not_ supported.
- `universal.UniversalKafkaQueue` to work with `universal.UniversalQueueMessage`, and `universal2.UniversalKafkaQueue` to work with `universal2.UniversalQueueMessage`.

### UniversalRedisQueue ###

Universal [Redis](http://redis.io) queue implementation.

`universal.UniversalRedisQueue` to work with `universal.UniversalQueueMessage`, and
`universal2.UniversalRedisQueue` to work with `universal2.UniversalQueueMessage`.

### UniversalRocksDbQueue ###

Universal queue implementation that uses [RocksDB](http://rocksdb.org) to store queue messages.

- Ephemeral storage is currently _not_ supported.
- `universal.UniversalRocksDbQueue` to work with `universal.UniversalRocksDbQueue`, and `universal2.UniversalKafkaQueue` to work with `universal2.UniversalQueueMessage`.


## License ##

See LICENSE.txt for details. Copyright (c) 2015-2016 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.

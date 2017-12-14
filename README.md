ddth-queue
==========

DDTH's libary to interact with various queue implementations.

Project home:
[https://github.com/DDTH/ddth-queue](https://github.com/DDTH/ddth-queue)


## Introduction

I work with queues from projects to projects.
However, different projects are fit with different queues.
I need a unified and simple API set to interact with various queue backend systems,
also extra functionalities such as commit queue item when done or re-queue the item if needed,
or find orphan queue items (items that have not been committed for a long period).
Hence this library is born to fulfill my need.


## Installation

Latest release version: `0.6.1.1`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency: if only a sub-set of `ddth-queue` functionality is used, choose the
corresponding dependency artifact(s) to reduce the number of unused jar files.

`ddth-queue-core`: ddth-queue interfaces and in-memory (using `java.util.Queue`) implementations:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-queue-core</artifactId>
	<version>0.6.1.1</version>
</dependency>
```

`ddth-queue-activemq`: include `ddth-queue-core` and [Apache ActiveMQ](http://activemq.apache.org) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-activemq</artifactId>
    <version>0.6.1.1</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-disruptor`: include `ddth-queue-core` and [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-disruptor</artifactId>
    <version>0.6.1.1</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-jdbc`: include `ddth-queue-core` and [`ddth-dao-jdbc`](https://github.com/DDTH/ddth-dao) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-jdbc</artifactId>
    <version>0.6.1.1</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-kafka`: include `ddth-queue-core` and [`ddth-kafka`](https://github.com/DDTH/ddth-kafka) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-kafka</artifactId>
    <version>0.6.1.1</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-rabbitmq`: include `ddth-queue-core` and [`RabbitMQ`](https://www.rabbitmq.com) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-rabbitmq</artifactId>
    <version>0.6.1.1</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-redis`: include `ddth-queue-core` and [`Jedis`](https://github.com/xetorthio/jedis) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-jedis</artifactId>
    <version>0.6.1.1</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-rocksdb`: include `ddth-queue-core` and [`RocksDB`](https://github.com/facebook/rocksdb) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-rocskdb</artifactId>
    <version>0.6.1.1</version>
    <type>pom</type>
</dependency>
```

## Usage

`ddth-queue` provides a unified and simple API to interact with various queue implementations:

- Put an item to queue: queue or re-queue.
- Take an item from queue.
- Retrieve list of orphan items.


### Queue Usage Flow

- Call `IQueue.take()` to take a message from queue.
- Do something with the message.
  - When done, call `IQueue.finish(msg)`
  - If not done and the message need to be re-queued, either call `IQueue.requeue(msg)` or `IQueue.requeueSilent(msg)` to put back the message to queue.

### Queue Storage Implementation

Queue implementation has 2 message storages:

- *Queue storage*: (required) main storage where messages are put into and taken from. Queue storage is implemented as FIFO list.
- *Ephemeral storage*: (optional) messages taken from queue storage are temporarily store in a ephemeral until _finished_ or _re-queued_.

(Queue implementation is required to provide *Queue storage*. *Ephemeral storage* is optional)

When `IQueue.take()` is called, the message is put in a ephemeral storage.
When either `IQueue.finish(msg)` or  `IQueue.requeue(msg)` or `IQueue.requeueSilent(msg)` is called,
the message is removed from the ephemeral storage.

The idea of the ephemeral storage is to make sure messages are not lost in the case the application
crashes in between `IQueue.take()` and `IQueue.finish(msg)` (or `IQueue.requeue(msg)`, or `IQueue.requeueSilent(msg)`).

#### Orphan Messages

If the application crashes in between `IQueue.take()` and `IQueue.finish(msg)` (or `IQueue.requeue(msg)`, or `IQueue.requeueSilent(msg)`)
there could be orphan messages left in the ephemeral storage. To deal with orphan messages:

- Call `Collection<IQueueMessage> getOrphanMessages(long thresholdTimestampMs)` to get all orphan messages that were queued _before_ `thresholdTimestampMs`.
- Call `IQueue.finish(msg)` to completely clear the orphan message from the ephemeral storage, or
- Call `IQueue.requeue(msg)`, or `IQueue.requeueSilent(msg)` to move the message back to the queue.


### APIs

Provided via `IQueue` interface:

| API                                               | Description |
|---------------------------------------------------|-------------|
|`boolean queue(IQueueMessage)`                     | Put a message to queue storage. | 
|`boolean requeue(IQueueMessage)`                   | Re-queue a taken message. Queue implementation must remove the message instance in the ephemeral storage (if any). Once re-queued, message's timestamp and number of re-queue times are updated. |
|`boolean requeueSilent(IQueueMessage)`             | Similar to API `requeue` but message's timestamp and number of re-queue times are _not_ updated. |
|`IQueueMessage take()`                             | Take a message from queue. |
|`Collection<IQueueMessage> getOrphanMessages(long)`| Gets all orphan messages (messages that were left in ephemeral storage for a long time). |
|`finish(IQueueMessage)`                            | Called to clean-up message from ephemeral storage. |
|`int queueSize()`                                  | Gets number of item current in queue storage. |
|`int ephemeralSize()`                              | Gets number of item current in ephemeral storage. |


## Built-in Queue Implementations

| Implementation | Bounded Size | Persistent | Ephemeral Storage | Multi-Clients |
|----------------|:------------:|:----------:|:-----------------:|:-------------:|
| ActiveMQ       | No           | Yes (*)    | No                | Yes           |
| Disruptor      | Yes          | No         | Yes               | No            |
| In-memory      | Optional     | No         | Yes               | No            |
| JDBC           | No           | Yes        | Yes               | Yes           |
| Kafka          | No           | Yes        | No                | Yes           |
| RabbitMQ       | No           | Yes (*)    | No                | Yes           |
| Redis          | No           | Yes (*)    | Yes               | Yes           |
| RocksDB        | No           | Yes        | Yes               | No            |

- *Bounded Size*: queue's size is bounded.
  - Currently only in-memory queue(s), including Disruptor implementation, support bounded queue size.
  - Databases (JDBC), ActiveMQ, RabbitMQ, Kafka, Redis and RocksDB queues are virtually limited only by hardware's capacity.
- *Persistent*: queue's items are persistent between JVM restarts.
  - ActiveMQ, RabbitMQ, Redis: persistency is configured at the corresponding backend service.
- *Ephemeral Storage*: supports retrieval of orphan messages.
- *Multi-Clients*: multi-clients can share a same queue backend storage.


### In-Memory Queue

There are 2 implementations:

- [InmemQueue.java](ddth-queue-core/src/main/java/com/github/ddth/queue/impl/InmemQueue.java):
uses a `java.util.Queue` as Queue storage and a `java.util.concurrent.ConcurrentMap` as Ephemeral storage. 
- [DisruptorQueue.java](ddth-queue-core/src/main/java/com/github/ddth/queue/impl/DisruptorQueue.java):
use [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) as Queue storage
and a `java.util.concurrent.ConcurrentMap` as Ephemeral storage.

Messages in in-memory queues are _not_ persistent between JVM restarts!

### ActiveMQ Queue

This queue implementation utilizes [Apache ActiveMQ](http://activemq.apache.org) as queue storage.

_Ephemeral storage is currently not supported!_

Queue messages are persistent.

See [ActiveMqQueue.java](ddth-queue-core/src/main/java/com/github/ddth/queue/impl/ActiveMqQueue.java).

### JDBC Queue

Queue messages are stored in RDBMS tables.

See [JdbcQueue.java](ddth-queue-core/src/main/java/com/github/ddth/queue/impl/JdbcQueue.java).

Queue messages are persistent.

### Kafka Queue

This queue implementation utilizes [Apache Kafka](http://kafka.apache.org) as queue storage.

_Ephemeral storage is currently not supported!_

Queue messages are persistent.

See [KafkaQueue.java](ddth-queue-core/src/main/java/com/github/ddth/queue/impl/KafkaQueue.java).

### RabbitMQ Queue

This queue implementation utilizes [RabbitMQ](https://www.rabbitmq.com) as queue storage.

_Ephemeral storage is currently not supported!_

Queue messages are persistent.

See [RabbitMqQueue.java](ddth-queue-core/src/main/java/com/github/ddth/queue/impl/RabbitMqQueue.java).

### Redis Queue

Queue storage and Ephemeral storage are implemented as a Redis hash and sorted set respectively. 
Also, a Redis list is used as a queue of message-ids.

Queue messages are persistent (depends on Redis server's configurations).

See [RedisQueue.java](ddth-queue-core/src/main/java/com/github/ddth/queue/impl/RedisQueue.java).

### RocksDB Queue

Queue messages are stored in [RocskDB](http://rocksdb.org).

_Ephemeral storage is currently not supported!_

Queue messages are persistent.

See [RocksDbQueue.java](ddth-queue-core/src/main/java/com/github/ddth/queue/impl/RocksDbQueue.java).


## Pre-made Convenient implementations

- `com.github.ddth.queue.impl.universal.base.*`: base implementations of universal queue message & queue implementations.
- `com.github.ddth.queue.impl.universal.msg.*` : universal queue message implementations
  - `UniversalIdIntQueueMessage`: message content is `byte[]` and message id is `Long`
  - `UniversalIdStrQueueMessage`: message content is `byte[]` and message id is `String`
- `com.github.ddth.queue.impl.universal.idint.*`: universal queue implementations, where queue's message's id is a `Long`.
- `com.github.ddth.queue.impl.universal.idstr.*`: universal queue implementations, where queue's message's id is a `String`.

### Universal queue message

Universal queue message implementation, with the following fields:

- `queue_id`: message's unique id
  - `com.github.ddth.queue.impl.universal.msg.UniversalIdIntQueueMessage`: queue-id is a 64-bit `Long`
  - `com.github.ddth.queue.impl.universal.msg.UniversalIdStrQueueMessage`: queue-id is a `String`
- `org_timestamp` (`java.util.Date`): timestamp when the message was queued for the first time
- `timestamp` (`java.util.Date`): last timestamp when the message was (re-)queued
- `num_requeues` (`int`): number of times the message has been re-queued
- `content` (`byte[]`): message's content

### UniversalInmemQueue

Universal in-memory queue implementation that uses `java.util.Queue` as Queue storage,
and `java.util.concurrent.ConcurrentMap` as Ephemeral storage.

`com.github.ddth.queue.impl.universal.idint.UniversalInmemQueue` to work with `UniversalIdIntQueueMessage`, and `com.github.ddth.queue.impl.universal.idstr.UniversalInmemQueue` to work with `UniversalIdStrQueueMessage`.

### UniversalActiveMqQueue

Universal queue implementation that uses [Apache ActiveMQ](http://activemq.apache.org) to store queue messages.

- Ephemeral storage is currently _not_ supported.
- `com.github.ddth.queue.impl.universal.idint.UniversalActiveMqQueue` to work with `UniversalIdIntQueueMessage`, and`com.github.ddth.queue.impl.universal.idstr.UniversalActiveMqQueue` to work with `UniversalIdStrQueueMessage`.

### UniversalDisruptorQueue

Universal in-memory queue implementation that uses [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) as Queue storage, and `java.util.concurrent.ConcurrentMap` as Ephemeral storage.

`com.github.ddth.queue.impl.universal.idint.UniversalDisruptorQueue` to work with `UniversalIdIntQueueMessage`, and`com.github.ddth.queue.impl.universal.idstr.UniversalDisruptorQueue` to work with `UniversalIdStrQueueMessage`.

### UniversalJdbcQueue

Universal JDBC queue implementation:

- 2 db tables: 1 for queue and 1 for ephemeral storages
- `com.github.ddth.queue.impl.universal.idint.UniversalJdbcQueue` to work with `UniversalIdIntQueueMessage`, and `com.github.ddth.queue.impl.universal.idstr.UniversalJdbcQueue` to work with `UniversalIdStrQueueMessage`
- Property `ephemeralDisabled` (default `false`): when set to `true` ephemeral storage is disabled
- Property `fifo` (default `true`): when set to `true` messages are taken in FIFO manner

Sample table schema for MySQL: see [sample_schema_universal.mysql.sql](sample-dbschema/sample_schema_universal.mysql.sql).

Sample table schema for PgSQL: see [sample_schema_universal.pgsql.sql](sample-dbschema/sample_schema_universal.pgsql.sql).

### UniversalSingleStorageJdbcQueue

Similar to `UniversalJdbcQueue`, but queue messages are partitioned so that multiple queues share a same storage.

- 2 db tables: 1 for queue and 1 for ephemeral storages
- Queue messages within storage are partitioned by queue name
- `com.github.ddth.queue.impl.universal.idint.UniversalSingleStorageJdbcQueue` to work with `UniversalIdIntQueueMessage`, and `com.github.ddth.queue.impl.universal.idstr.UniversalSingleStorageJdbcQueue` to work with `UniversalIdStrQueueMessage`

Sample table schema for MySQL: see [sample_schema_universal_singlestore.mysql.sql](sample-dbschema/sample_schema_universal_singlestore.mysql.sql).

Sample table schema for PgSQL: see [sample_schema_universal_singlestore.pgsql.sql](sample-dbschema/sample_schema_universal_singlestore.pgsql.sql).

### LessLockingUniversalMySQLQueue

Similar to `UniversalJdbcQueue`, but using a less-locking algorithm - specific for MySQL, and needs
only one single db table for both queue and ephemeral storages.

- Optimized for MySQL
- 1 single db table for both queue and ephemeral storages
- `com.github.ddth.queue.impl.universal.idint.LessLockingUniversalMySQLQueue` to work with `UniversalIdIntQueueMessage`, and `com.github.ddth.queue.impl.universal.idstr.LessLockingUniversalMySQLQueue` to work with `UniversalIdStrQueueMessage`

Sample table schema for MySQL: see [sample_schema-less-locking-universal.mysql.sql](sample-dbschema/sample_schema-less-locking-universal.mysql.sql).

### LessLockingUniversalPgSQLQueue

Similar to `UniversalJdbcQueue`, but using a less-locking algorithm - specific for PostgreSQL, and needs
only one single db table for both queue and ephemeral storages.

- Optimized for PostgreSQL
- 1 single db table for both queue and ephemeral storages
- `com.github.ddth.queue.impl.universal.idint.LessLockingUniversalPgSQLQueue` to work with `UniversalIdIntQueueMessage`, and `com.github.ddth.queue.impl.universal.idstr.LessLockingUniversalPgSQLQueue` to work with `UniversalIdStrQueueMessage`

Sample table schema for MySQL: see [sample_schema-less-locking-universal.pgsql.sql](sample-dbschema/sample_schema-less-locking-universal.pgsql.sql).

### LessLockingUniversalSingleStorageMySQLQueue

Similar to `UniversalSingleStorageJdbcQueue`, but using a less-locking algorithm - specific for MySQL, and needs
only one single db table for both queue and ephemeral storages.

- Optimized for MySQL
- 1 single db table for both queue and ephemeral storages
- Queue messages within storage are partitioned by queue name
- `com.github.ddth.queue.impl.universal.idint.LessLockingUniversalSingleStorageMySQLQueue` to work with `UniversalIdIntQueueMessage`, and `com.github.ddth.queue.impl.universal.idstr.LessLockingUniversalSingleStorageMySQLQueue` to work with `UniversalIdStrQueueMessage`

Sample table schema for MySQL: see [sample_schema-less-locking-universal-singlestore.mysql.sql](sample-dbschema/sample_schema-less-locking-universal-singlestore.mysql.sql).

### LessLockingUniversalSingleStoragePgSQLQueue

Similar to `UniversalSingleStorageJdbcQueue`, but using a less-locking algorithm - specific for PostgreSQL, and needs
only one single db table for both queue and ephemeral storages.

- Optimized for PostgreSQL
- 1 single db table for both queue and ephemeral storages
- Queue messages within storage are partitioned by queue name
- `com.github.ddth.queue.impl.universal.idint.LessLockingUniversalSingleStoragePgSQLQueue` to work with `UniversalIdIntQueueMessage`, and `com.github.ddth.queue.impl.universal.idstr.LessLockingUniversalSingleStoragePgSQLQueue` to work with `UniversalIdStrQueueMessage`

Sample table schema for MySQL: see [sample_schema-less-locking-universal-singlestore.pgsql.sql](sample-dbschema/sample_schema-less-locking-universal-singlestore.pgsql.sql).

### UniversalKafkaQueue

Universal queue implementation that uses [Apache Kafka](http://kafka.apache.org) as queue backend.

- Ephemeral storage is currently _not_ supported.
- `com.github.ddth.queue.impl.universal.idint.UniversalKafkaQueue` to work with `UniversalIdIntQueueMessage`, and `com.github.ddth.queue.impl.universal.idstr.UniversalKafkaQueue` to work with `UniversalIdStrQueueMessage`.

### UniversalRabbitMqQueue

Universal queue implementation that uses [RabbitMQ](https://www.rabbitmq.com) to store queue messages.

- Ephemeral storage is currently _not_ supported.
- `com.github.ddth.queue.impl.universal.idint.UniversalRabbitMqQueue` to work with `UniversalIdIntQueueMessage`, and`com.github.ddth.queue.impl.universal.idstr.UniversalRabbitMqQueue` to work with `UniversalIdStrQueueMessage`.

### UniversalRedisQueue

Universal queue implementation that uses [Redis](http://redis.io) to store queue messages.

`com.github.ddth.queue.impl.universal.idint.UniversalRedisQueue` to work with `UniversalIdIntQueueMessage`, and
`com.github.ddth.queue.impl.universal.idstr.UniversalRedisQueue` to work with `UniversalIdStrQueueMessage`.

### UniversalRocksDbQueue

Universal queue implementation that uses [RocksDB](http://rocksdb.org) to store queue messages.

- Ephemeral storage is currently _not_ supported.
- `com.github.ddth.queue.impl.universal.idint.UniversalRocksDbQueue` to work with `UniversalIdIntQueueMessage`, and `com.github.ddth.queue.impl.universal.idstr.UniversalRocksDbQueue` to work with `UniversalIdStrQueueMessage`.


## License

See LICENSE.txt for details. Copyright (c) 2015-2017 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.

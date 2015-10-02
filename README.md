ddth-queue
==========

DDTH's libary to interact with various queue implementations.

Project home:
[https://github.com/DDTH/ddth-queue](https://github.com/DDTH/ddth-queue)

OSGi environment: ddth-queue modules are packaged as an OSGi bundle (Experimental!).


## Installation ##

Latest release version: `0.3.3`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-queue</artifactId>
	<version>0.3.3</version>
</dependency>
```

## Usage ##

`ddth-queue` provides a unified APIs to interact with various queue implementations.

### Queue Usage Flow ###

- Call `IQueue.take()` to take a message from queue.
- Do something with the message.
  - When done, call `IQueue.finish(msg)`
  - If not done and the message need to be re-queued, either call `IQueue.requeue(msg)` or `IQueue.requeueSilent(msg)` to put back the message to queue.

### Queue Storage Implementation ###

Queue implementation has 2 message storages:

- *Queue storage*: (required) main storage where messages are put into and taken from. Queue storage is FIFO.
- *Ephemeral storage*: (optional) messages taken from queue storage are temporarily store in a ephemeral until _finished_ or _re-queued_.

(Queue implementation is required to provide *Queue storage*. *Ephemeral storage* is optional.)

When `IQueue.take()` is called, the message is put in a ephemeral storage.
When either `IQueue.finish(msg)` or  `IQueue.requeue(msg)` or `IQueue.requeueSilent(msg)` is called,
the message is removed from the ephemeral storage.

The idea of the ephemeral storage is to make sure messages are not lost in the case the application
crashes in between `IQueue.take()` and `IQueue.finish(msg)` (or `IQueue.requeue(msg)`, or `IQueue.requeueSilent(msg)`).

#### Orphan Messages ####

If the application crashes in between `IQueue.take()` and `IQueue.finish(msg)` (or `IQueue.requeue(msg)`, or `IQueue.requeueSilent(msg)`)
there could be orphan messages left in the ephemeral storage. To deal with orphan messages:

- Call `Collection<IQueueMessage> getOrphanMessages(long thresholdTimestampMs)` to get all orphan messages that were queued _before_ `thresholdTimestampMs`.
- Call `IQueue.finish(msg)` to clear the orphan message from the ephemeral storage, or
- Call `IQueue.requeue(msg)`, or `IQueue.requeueSilent(msg)` to put the message back to the queue.


### APIs ###

*`boolean IQueue.queue(IQueueMessage)`*: Put a message to queue storage.

*`boolean requeue(IQueueMessage)`*: Re-queue a taken message. Queue implementation must remove the message instance in the ephemeral storage (if any). Once re-queued, message's timestamp and number of re-queue times are updated.

*`boolean requeueSilent(IQueueMessage)`*: Similar to API `requeue` but message's timestamp and number of re-queue times are _not_ updated.

*`IQueueMessage take()`*: Take a message from queue.

*`Collection<IQueueMessage> getOrphanMessages(long)`*: Gets all orphan messages (messages that were left in ephemeral storage for a long time).

*`finish(IQueueMessage)`*: Called to clean-up message from ephemeral storage.

*`int queueSize()`*: Gets queue's number of items.

*`int ephemeralSize()`*: Gets ephemeral-storage's number of items.


## Queue Implementations ##

### JDBC Queue ###

Queue storage and Ephemeral storage are implemented as 2 database tables, identical schema.

See [JdbcQueue.java](src/main/java/com/github/ddth/queue/impl/JdbcQueue.java).

Usage:

- Extends class `con.github.ddth.queue.impl.JdbcQueue`, and
- Implements the following methods:
    - `protected IQueueMessage readFromQueueStorage(JdbcTemplate jdbcTemplate)`
    - `protected IQueueMessage readFromEphemeralStorage(JdbcTemplate jdbcTemplate, IQueueMessage msg)`
    - `protected Collection<IQueueMessage> getOrphanFromEphemeralStorage(JdbcTemplate jdbcTemplate, long thresholdTimestampMs)`
    - `protected boolean putToQueueStorage(JdbcTemplate jdbcTemplate, IQueueMessage msg)`
    - `protected boolean putToEphemeralStorage(JdbcTemplate jdbcTemplate, IQueueMessage msg)`
    - `protected boolean removeFromQueueStorage(JdbcTemplate jdbcTemplate, IQueueMessage msg)`
    - `protected boolean removeFromEphemeralStorage(JdbcTemplate jdbcTemplate, IQueueMessage msg)`


### Redis Queue ###

Queue storage and Ephemeral storage are implemented as a Redis hash and sorted set respectively. 
Also, a Redis list is used as a queue of message-ids.

See [RedisQueue.java](src/main/java/com/github/ddth/queue/impl/RedisQueue.java)

Usage:

- Extends class `con.github.ddth.queue.impl.RedisQueue`, and
- Implements the following methods:
    - `protected byte[] serialize(IQueueMessage msg)`
    - `protected IQueueMessage deserialize(byte[] msgData)`


## Pre-made Convenient Classes ##

### UniversalQueueMessage ###

Universal queue message implementation, with the following fields:

- `queue_id` (`long`): message's unique id in the queue
- `org_timestamp` (`java.util.Date`): timestamp when the message was first-queued
- `timestamp` (`java.util.Date`): message's last-queued timestamp
- `num_requeues` (`int`): number of times the message has been re-queued
- `content` (`byte[]`): message's content

### UniversalRedisQueue ###

Universal Redis queue implementation:

- Work with `UniversalQueueMessage`
- Use a hash (to store `{queue_id => message}`), a list (to act as a queue of `queue_id`), and a sorted-set (to act as the ephemeral storage).


### UniversalJdbcQueue ###

Universal JDBC queue implementation:

- 2 db tables for queue and ephemeral storages
- Work with `UniversalQueueMessage`
- Property `ephemeralDisabled` (default `false`): when set to `true` ephemeral storage is disabled
- Property `fifo` (default `true`): when set to `true` messages are taken in FIFO manner

Sample table schema for MySQL: see [sample_schema.mysql.sql](sample-dbschema/sample_schema.mysql.sql).

Sample table schema for PgSQL: see [sample_schema.pgsql.sql](sample-dbschema/sample_schema.pgsql.sql).

### LessLockingUniversalMySQLQueue ###

Similar to `UniversalJdbcQueue`, but using a less-locking algorithm - specific for MySQL, and requires
only one single db table for both queue and ephemeral storages.

- Optimized for MySQL (EXPERIMENTAL!)
- 1 single db table for both queue and ephemeral storages
- Work with `UniversalQueueMessage`
- Property `fifo` (default `true`): when set to `true` messages are taken in FIFO manner

Sample table schema for MySQL: see [sample_schema-less-locking.mysql.sql](sample-dbschema/sample_schema-less-locking.mysql.sql).

### LessLockingUniversalPgSQLQueue ###

Similar to `UniversalJdbcQueue`, but using a less-locking algorithm - specific for PostgreSQL, and requires
only one single db table for both queue and ephemeral storages.

- Optimized for PostgreSQL (EXPERIMENTAL!)
- 1 single db table for both queue and ephemeral storages
- Work with `UniversalQueueMessage`
- Property `fifo` (default `true`): when set to `true` messages are taken in FIFO manner

Sample table schema for MySQL: see [sample_schema-less-locking.pgsql.sql](sample-dbschema/sample_schema-less-locking.pgsql.sql).


## License ##

See LICENSE.txt for details. Copyright (c) 2015 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.

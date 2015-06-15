ddth-queue
==========

DDTH's libary to interact with various queue implementations.

Project home:
[https://github.com/DDTH/ddth-queue](https://github.com/DDTH/ddth-queue)

OSGi environment: ddth-queue modules are packaged as an OSGi bundle.


## Installation ##

Latest release version: `0.2.2.2`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-queue</artifactId>
	<version>0.2.2.1</version>
</dependency>
```

## Usage ##

`ddth-queue` provides a unified APIs to interact with various queue implementations.

### Queue Usage Flow ###

- Call `IQueue.take()` to take a message from queue.
- Do something with the message.
  - When done, call `IQueue.finish(msg)`
  - If not done and the message need to be re-queued, either call `IQueue.requeue(msg)` or `IQueue.requeueSilent(msg)` to put back the message to queue.


#### Orphan Messages ####

If the application crashes in between `IQueue.take()` and `IQueue.finish(msg)` (or `IQueue.requeue(msg)`, or `IQueue.requeueSilent(msg)`)
there could be orphan messages left in the ephemeral storage. To deal with orphan messages:

- Call `Collection<IQueueMessage> getOrphanMessages(long thresholdTimestampMs)` to get all orphan messages that were queued _before_ `thresholdTimestampMs`.
- Call `IQueue.finish(msg)` to clear the orphan message from the ephemeral storage, or
- Call `IQueue.requeue(msg)`, or `IQueue.requeueSilent(msg)` to put the message back to the queue.


### Queue Storage Implementation ###

Queue implementation has 2 message storages:

- *Queue storage*: (required) main storage where messages are put into and taken from. Queue storage is FIFO.
- *Ephemeral storage*: (optional) messages taken from queue storage are temporarily store in a ephemeral until _finished_ or _re-queued_.

Queue implementation is required to provide *Queue storage*. *Ephemeral storage* is optional.


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

Usage:

- Extends class `con.github.ddth.queue.impl.JdbcQueue`, and
- Implements 6 methods:
  - `IQueueMessage readFromQueueStorage(JdbcTemplate)`
  - `Collection<IQueueMessage> getOrphanFromEphemeralStorage(JdbcTemplate, long)`
  - `boolean putToQueueStorage(JdbcTemplate, IQueueMessage)`
  - `boolean putToEphemeralStorage(JdbcTemplate, IQueueMessage)`
  - `boolean removeFromQueueStorage(JdbcTemplate, IQueueMessage)`
  - `boolean removeFromEphemeralStorage(JdbcTemplate, IQueueMessage)`

  
## License ##

See LICENSE.txt for details. Copyright (c) 2015 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.

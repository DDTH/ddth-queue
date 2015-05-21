ddth-queue
==========

DDTH's libary to interact with various queue implementations.

Project home:
[https://github.com/DDTH/ddth-queue](https://github.com/DDTH/ddth-queue)

OSGi environment: ddth-queue modules are packaged as an OSGi bundle.


## License ##

See LICENSE.txt for details. Copyright (c) 2015 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.


## Installation #

Latest release version: `0.1.3`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-queue</artifactId>
	<version>0.1.2</version>
</dependency>
```

## Usage ##

`ddth-queue` provides a unified APIs to interact with various queue implementations.

### Queue Usage Flow ###

- Call `IQueue.take()` to take a message from queue.
- Do something with the message.
  - When done, call `IQueue.finish(msg)`
  - If not done and the message need to be requeue, either call `IQueue.requeue(msg)` or `IQueue.requeueSilent(msg)` to put backthe message to queue.

### Queue Storage Implementation ###

Queue implementation has 2 message storages:
- *Queue storage*: (required) main storage where messages are put into and taken from. Queue storage is FIFO.
- *Ephemeral storage*: (optional) messages taken from queue storage are temporarily store in a ephemeral until _finished_ or _re-queued_.

Queue implementation is required to provide *Queue storage* but *Ephemeral storage* is optional.

### API Implementation ###

*`boolean IQueue.queue(IQueueMessage)`*: Put a message to queue storage.

*`boolean requeue(IQueueMessage)`*: Re-queue a taken message. Queue implementation must remove the message instance in the ephemeral storage (if any). Once re-queued, message's timestamp and number of re-queue times are updated.

*`boolean requeueSilent(IQueueMessage)`*: Similar to API `requeue` but message's timestamp and number of re-queue times are _not_ updated.

*`IQueueMessage take()`*: Take a message from queue.

*`finish(IQueueMessage)`*: Called to clean-up message from ephemeral storage.

## Queue Implementations ##

### JDBC Queue ###

Queue storage and Ephemeral storage are implemented as 2 database tables, identical schema. 

Usage:

- Extends class `con.github.ddth.queue.impl.JdbcQueue`, and
- Implements 6 methods:
  - `IQueueMessage readFromQueueStorage(JdbcTemplate)`
  - `IQueueMessage readFromEphemeralStorage(JdbcTemplate)`
  - `boolean putToEphemeralStorage(JdbcTemplate, IQueueMessage)`
  - `boolean removeFromQueueStorage(JdbcTemplate, IQueueMessage)`
  - `boolean removeFromEphemeralStorage(JdbcTemplate, IQueueMessage)`

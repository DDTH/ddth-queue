# Message Publish/Subscribe Functionality

Since v0.7.0, `ddth-queue` provides a unified API to publish/subscribe messages:

- Publish messages to channels.
- Subscribe to channels and consume messages.


### Pub/Sub Usage Flow

- Create a `IPubSubHub` instance. Pre-made implementations can be used out-of-the-box, see below.
- Call `IPubSubHub.publish(channel, msg)` publish a message to a channel.
- Call `IPubSubHub.subscribe(channel, subscriber)` to subscribe to a channel:
  - Multiple subscribers can subscribe to one channel.
  - One subscriber instance can subscribe to multiple channels.
- Call `IPubSubHub.subscribe(channel, subscriber)` to unsubscribe from a channel.


### APIs

Provided via `IPubSubHub` interface:

| API                                               | Description |
|---------------------------------------------------|-------------|
|`boolean publish(String, IMessage)`                | Publish a message to a channel. |
|`void subscribe(String, ISubscriber)`              | Subscribe to a channel to receive messages. |
|`void unsubscribe(String, ISubscriber)`            | Unsubscribe from a channel. |


## Built-in pub-sub Implementations

| Implementation | Inter-process |
|----------------|:-------------:|
| In-memory      | No            |
| Redis          | Yes           |

- *Inter-process*: publishers and subscribers can be from different JVMs.


### In-Memory Pub/Sub

Publishers and Subscribers are on the same JVM.

See [InmemPubSubHub.java](ddth-queue-core/src/main/java/com/github/ddth/pubsub/impl/InmemPubSubHub.java).

### Redis Pub/Sub

Utilize Redis' pub/sub to distribute messages.

Publishers and Subscribers can be on different JVMs.

See [RedisPubSubHub.java](ddth-queue-core/src/main/java/com/github/ddth/pubsub/impl/RedisPubSubHub.java).


## Pre-made Convenient implementations

- `com.github.ddth.pubsub.impl.universal.*`:
  - `UniversalIdIntMessage`: "universal" implementation of message where content is `byte[]` and id is `Long`
  - `UniversalIdStrMessage`: "universal" implementation of message where content is `byte[]` and id is `String`
  - Factory to create `UniversalIdIntMessage` and `UniversalIdStrMessage`
- `com.github.ddth.pubsub.impl.universal.idint.*`: universal pub/sub implementations, where message's id is `Long`.
- `com.github.ddth.pubsub.impl.universal.idstr.*`: universal pub/sub implementations, where message's id is `String`.

### Universal message

"Universal" message implementation, with the following fields:

- `id`: message's unique id
  - `com.github.ddth.pubsub.impl.universal.UniversalIdIntMessage`: id is a 64-bit `Long`
  - `com.github.ddth.pubsub.impl.universal.UniversalIdStrMessage`: id is a `String`
- `time` (`java.util.Date`): timestamp when the message was created
- `data` (`byte[]`): message's content
- `pkey` (`String`): message's partition key

### CountingSubscriber

A subscriber that counts number of received messages. See [CountingSubscriber.java](ddth-queue-core/src/main/java/com/github/ddth/pubsub/impl/CountingSubscriber.java).

### UniversalInmemPubSubHub

Universal in-memory pub/sub implementation: publishers and subscribers need to be on the same JVM.

`com.github.ddth.pubsub.impl.universal.idint.UniversalInmemPubSubHub` to work with `UniversalIdIntMessage`,
and `com.github.ddth.pubsub.impl.universal.idstr.UniversalInmemPubSubHub` to work with `UniversalIdStrMessage`.

### UniversalRedisPubSubHub

Universal pub/sub implementation that uses [Redis](http://redis.io) to transit messages: publishers and subscribers can be on different JVMs.

`com.github.ddth.pubsub.impl.universal.idint.UniversalRedisPubSubHub` to work with `UniversalIdIntMessage`,
and `com.github.ddth.pubsub.impl.universal.idstr.UniversalRedisPubSubHub` to work with `UniversalIdStrMessage`.

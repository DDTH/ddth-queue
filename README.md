# ddth-queue

DDTH's libary to interact with various queue implementations.

Project home:
[https://github.com/DDTH/ddth-queue](https://github.com/DDTH/ddth-queue)


## Introduction

I work with queues from projects to projects.
However, different projects use different queue implementations (based on their needs and context).
I need a unified and simple API set to interact with various queue backend systems,
also extra functionalities such as commit queue messages when done or re-queue messages if needed,
or find orphan ones (messages that have not been committed for a long period).
Hence this library is born to fulfill my need.

**Since v0.7.0** `ddth-queue` added pub/sub functionality.


## Installation

Latest release version: `0.7.1.2`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency: if only a sub-set of `ddth-queue` functionality is used, choose the
corresponding dependency artifact(s) to reduce the number of unused jar files.

`ddth-queue-core`: ddth-queue interfaces and in-memory (using `java.util.Queue`) implementations:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-queue-core</artifactId>
	<version>0.7.1.2</version>
</dependency>
```

`ddth-queue-activemq`: include `ddth-queue-core` and [Apache ActiveMQ](http://activemq.apache.org) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-activemq</artifactId>
    <version>0.7.1.2</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-disruptor`: include `ddth-queue-core` and [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-disruptor</artifactId>
    <version>0.7.1.2</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-jdbc`: include `ddth-queue-core` and [`ddth-dao-jdbc`](https://github.com/DDTH/ddth-dao) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-jdbc</artifactId>
    <version>0.7.1.2</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-kafka`: include `ddth-queue-core` and [`ddth-kafka`](https://github.com/DDTH/ddth-kafka) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-kafka</artifactId>
    <version>0.7.1.2</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-rabbitmq`: include `ddth-queue-core` and [`RabbitMQ`](https://www.rabbitmq.com) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-rabbitmq</artifactId>
    <version>0.7.1.2</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-redis`: include `ddth-queue-core` and [`Jedis`](https://github.com/xetorthio/jedis) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-jedis</artifactId>
    <version>0.7.1.2</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-rocksdb`: include `ddth-queue-core` and [`RocksDB`](https://github.com/facebook/rocksdb) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-rocskdb</artifactId>
    <version>0.7.1.2</version>
    <type>pom</type>
</dependency>
```


## Usage

See [PUBSUB.md](PUBSUB.md) for message publish/subscribe functionality.

See [QUEUE.md](QUEUE.md) for message queue functionality.


## License

See LICENSE.txt for details. Copyright (c) 2015-2018 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.

[![Build Status](https://travis-ci.org/DDTH/ddth-queue.svg?branch=master)](https://travis-ci.org/DDTH/ddth-queue) [![Javadocs](http://javadoc.io/badge/com.github.ddth/ddth-queue-core.svg)](http://javadoc.io/doc/com.github.ddth/ddth-queue-core)

# ddth-queue

DDTH's libary to interact with various queue implementations.

Project home:
[https://github.com/DDTH/ddth-queue](https://github.com/DDTH/ddth-queue)

`ddth-queue` requires Java 11+ since v1.0.0. For Java 8, use v0.7.x


## Introduction

I work with queues from project to project.
However, different projects use different queue implementations (based on their needs and context).
I need a unified and simple API set to interact with various queue backend systems,
also extra functionality such as commit queue messages when done or re-queue messages if needed,
or find orphan ones (messages that have not been committed for a long period).
Hence this library is born to fulfill my need.

**Since v0.7.0** `ddth-queue` added pub/sub functionality.


## Installation

Latest release version: `1.0.0`. See [RELEASE-NOTES.md](RELEASE-NOTES.md).

Maven dependency: if only a sub-set of `ddth-queue` functionality is used, choose the
corresponding dependency artifact(s) to reduce the number of unused jar files.

`ddth-queue-core`: ddth-queue interfaces and in-memory (using `java.util.Queue`) implementation:

```xml
<dependency>
	<groupId>com.github.ddth</groupId>
	<artifactId>ddth-queue-core</artifactId>
	<version>1.0.0</version>
</dependency>
```

`ddth-queue-activemq`: include `ddth-queue-core` and [Apache ActiveMQ](http://activemq.apache.org) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-activemq</artifactId>
    <version>1.0.0</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-disruptor`: include `ddth-queue-core` and [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-disruptor</artifactId>
    <version>1.0.0</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-jdbc`: include `ddth-queue-core` and [`ddth-dao-jdbc`](https://github.com/DDTH/ddth-dao) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-jdbc</artifactId>
    <version>1.0.0</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-kafka`: include `ddth-queue-core` and [`ddth-kafka`](https://github.com/DDTH/ddth-kafka) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-kafka</artifactId>
    <version>1.0.0</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-rabbitmq`: include `ddth-queue-core` and [`RabbitMQ`](https://www.rabbitmq.com) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-rabbitmq</artifactId>
    <version>1.0.0</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-redis`: include `ddth-queue-core` and [`Jedis`](https://github.com/xetorthio/jedis) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-redis</artifactId>
    <version>1.0.0</version>
    <type>pom</type>
</dependency>
```

`ddth-queue-rocksdb`: include `ddth-queue-core` and [`RocksDB`](https://github.com/facebook/rocksdb) dependencies:

```xml
<dependency>
    <groupId>com.github.ddth</groupId>
    <artifactId>ddth-queue-rocskdb</artifactId>
    <version>1.0.0</version>
    <type>pom</type>
</dependency>
```


## Usage

See [PUBSUB.md](PUBSUB.md) for message publish/subscribe functionality.

See [QUEUE.md](QUEUE.md) for message queue functionality.


## License

See LICENSE.txt for details. Copyright (c) 2015-2019 Thanh Ba Nguyen.

Third party libraries are distributed under their own licenses.

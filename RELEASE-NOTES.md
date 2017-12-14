# ddth-queue release notes

## 0.6.1 - 2017-12-14

- New queue implementations: `ActiveMQ` & `RabbitMQ`.
- Bug fixes and enhancements.


## 0.6.0 - 2017-12-11

- Refactoring:
  - `IQueue` and `IQueueMessage` are now generic-typed.
  - Universal queues:
    - Queue message's content remains `byte[]`
    - Queue message's id is `Long`: moved to package `com.github.ddth.queue.impl.universal.idint`
    - Queue message's id is `String`: moved to package `com.github.ddth.queue.impl.universal.idstr`
  - New classes:
    - `GenericQueueMessage`: universal-generic implementation of `IQueueMessage`
    - `IQueueObserver`: interface to observe queue's actions
    - `NoopQueueObserver`: no-op implementation of `IQueueObserver`
- New queue implementations:
  - `UniversalSingleStorageJdbcQueue`: messages from multiple queues are stored in same storage.
  - `LessLockingUniversalSingleStorageMySQLQueue`: same as `UniversalSingleStorageJdbcQueue` but optimized for MySQL
  - `LessLockingUniversalSingleStoragePgSQLQueue`: same as `UniversalSingleStorageJdbcQueue` but optimized for PostgreSQL
- More unit tests.
- Update dependencies.
- Bug fixes and enhancements.


## 0.5.1.1 - 2017-10-29

- Upgrade dependencies and libs.
- Refactor: `JdbcQueue`-based queues now use `IJdbcHelper` instead of `JdbcTemplate`


## 0.5.1 - 2017-02-12

- Upgrade `ddthk-kafka` to v1.3.3
- Bug fixes & enhancements.
- Add more unit tests.


## 0.5.0 - 2017-02-10

- Bump to `com.github.ddth:ddth-parent:6`, now requires Java 8+.
- Upgrade RocksDb version to 5.0.1, LMAX Disruptor to 3.3.6, ddtj-kafka to 1.3.2
- Refactor & Enhancements:
  - New interface `IPartitionSupport`
  - New abstract classes `AbstractQueue` and `AbstractEphemeralSupportQueue`
  - Move base class for universal queues to a separated package `com.github.ddth.queue.impl.base`
- Add more unit tests.


## 0.4.2 - 2016-06-23

- New methods:
  - `IQueueMessage IQueueMessage.qData(Object)`: attach content/data to the queue message.
  - `Object IQueueMessage.qData()`: retrieve the attached content/data from queue message.


## 0.4.1.1 - 2016-06-14

- Bug fixed: FIFO for JDBC-based queues.


## 0.4.1 - 2016-06-08

- Factories to create queue instances.


## 0.4.0.1 - 2016-05-12

- (Experimental) `RocksDbQueue`: support ephemeral storage functionality.
- Bug fixes.
- More unit tests.


## 0.4.0 - 2016-05-10

- Separated artifacts: `ddth-queue-core`, `ddth-queue-disruptor`, `ddth-queue-jdbc`, `ddth-queue-kafka` and `ddth-queue-redis`.
- (Experimental) New queue implementation that use [Facebook RocksDB](http://rocksdb.org) as queue backend.
- New in-memory queue implementations:
  - `InmemQueue` that uses `java.util.Queue` as queue backend.
  - `DisruptorQueue` that uses [LMAX Disruptor](https://lmax-exchange.github.io/disruptor/) as queue backend.
- Ephemeral storage can be optionally disabled for `InmemQueue`, `DisruptorQueue` and `RedisQueue`.


## 0.3.3.2 - 2015-10-05

- (Experimental) Universal Kafka queue:
  - Class `BaseUniversalQueueMessage`: new field `kafkaKey` for partitioning queue messages.


## 0.3.3 - 2015-10-02

- (Experimental) Kafka queue:
  - Add `take()` operation.
  - Improve consumer performance.
- New package `com.github.ddth.queue.impl.universal`: universal queue implementations are moved to under this package.
  - `com.github.ddth.queue.impl.universal.UniversalQueueMessage`: queue-id is a 64-bit integer.
- New package `com.github.ddth.queue.impl.universal2`: universal queue implementations are moved to under this package.
  - `com.github.ddth.queue.impl.universal2.UniversalQueueMessage`: queue-id is a 32-hex-character string.


## 0.3.2 - 2015-09-02

- (Experimental) New queue implementation that use [Apache Kafka](http://kafka.apache.org) as queue backend.


## 0.3.0 - 2015-06-29

- New class `UniversalRedisQueue`: queue implementation with Redis backend.
- Bugs fixed and enhancements.


## 0.2.3.1 - 2015-06-28

- `JdbcQueue` enhancements
  - New property: `maxRetries` (default value: 3)
  - New property: `transactionIsolationLevel` (default value: `Connection.TRANSACTION_READ_COMMITTED`)
  - Retry if `PessimisticLockingFailureException`
- New convenient class `UniversalJdbcQueue`:
  - 2 db tables for queue and ephemeral storages
  - Work with `UniversalQueueMessage`
  - Property `ephemeralDisabled` (default `false`): when set to `true` ephemeral storage is disabled
  - Property `fifo` (default `true`): when set to `true` messages are taken in FIFO manner
- New convenient class `LessLockingUniversalMySQLQueue` (EXPERIMENTAL!!!):
  - Optimized for MySQL
  - 1 single db table for both queue and ephemeral storages
  - Work with `UniversalQueueMessage`
  - Property `fifo` (default `true`): when set to `true` messages are taken in FIFO manner
- New convenient class `LessLockingUniversalPgSQLQueue` (EXPERIMENTAL!!!):
  - Optimized for PgSQL
  - 1 single db table for both queue and ephemeral storages
  - Work with `UniversalQueueMessage`
  - Property `fifo` (default `true`): when set to `true` messages are taken in FIFO manner
- Other bug fixes & enhancements.


## 0.2.2.2 - 2015-06-15

Bug fixes & enhancements.


## 0.2.2 - 2015-06-12

New class `UniversalQueueMessage`.


## 0.2.1 - 2015-06-08

- Orphan messages enhancement:
  - New method `boolean IQueue.moveFromEphemeralToQueueStorage(IQueueMessage)`
  - New method `IQueueMessage readFromEphemeralStorage(JdbcTemplate, IQueueMessage)`


## 0.2.0 - 2015-06-05

- New method `IQueue.getOrphanMessages(long)`
- New method `abstract JdbcQueue.readOrphanFromEphemeralStorage(JdbcTemplate, long)`
- Remove method `abstract JdbcQueue.readFromEphemeralStorage(JdbcTemplate)`


## 0.1.3 - 2015-05-22

- Fix a bug with `DuplicateKeyException`.
- Minor fixes & enhancements.


## 0.1.1 - 2015-05-09

Bugs fixed & enhancements.


## 0.1.0 - 2015-03-01

First release: API interface & Jdbc queue implementation.

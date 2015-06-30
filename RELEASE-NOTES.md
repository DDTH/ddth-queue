ddth-queue release notes
========================

0.3.1 - 2015-06-30
------------------

- Enhancements:
  - `UniversalRedisQueue` now extends an abtract class `RedisQueue`.
  - Many `RedisQueue`s can share a single `JedisPool`.


0.3.0 - 2015-06-29
------------------

- New class `UniversalRedisQueue`: queue implementation with Redis backend.
- Bugs fixed and enhancements.


0.2.3.1 - 2015-06-28
--------------------

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


0.2.2.2 - 2015-06-15
--------------------
Bug fixes & enhancements.


0.2.2 - 2015-06-12
------------------
New class `UniversalQueueMessage`.


0.2.1 - 2015-06-08
------------------

- Orphan messages enhancement:
  - New method `boolean IQueue.moveFromEphemeralToQueueStorage(IQueueMessage)`
  - New method `IQueueMessage readFromEphemeralStorage(JdbcTemplate, IQueueMessage)`


0.2.0 - 2015-06-05
------------------

- New method `IQueue.getOrphanMessages(long)`
- New method `abstract JdbcQueue.readOrphanFromEphemeralStorage(JdbcTemplate, long)`
- Remove method `abstract JdbcQueue.readFromEphemeralStorage(JdbcTemplate)`


0.1.3 - 2015-05-22
------------------

- Fix a bug with `DuplicateKeyException`.
- Minor fixes & enhancements.


0.1.1 - 2015-05-09
------------------
Bugs fixed & enhancements.


0.1.0 - 2015-03-01
------------------
First release: API interface & Jdbc queue implementation.

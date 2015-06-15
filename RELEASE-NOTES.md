ddth-queue release notes
========================

0.2.2.1 - 2015-06-15
--------------------
- Bug fix.


0.2.2 - 2015-06-12
------------------
- New class `UniversalQueueMessage`.


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
- Bugs fixed & enhancements.


0.1.0 - 2015-03-01
------------------
- First release: API interface & Jdbc queue implementation.

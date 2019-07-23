package com.github.ddth.queue.test.universal.idint.mysql;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ 
    TestMySQLLLQueue.class,
    TestMySQLLLQueueLong.class,
    TestMySQLLLQueueLongBoundEphemeralSize.class,
    TestMySQLLLQueueLongEphemeralDisabled.class,

    TestMySQLLLSingleStorageQueue.class,
    TestMySQLLLSingleStorageQueueLong.class,
    TestMySQLLLSingleStorageQueueLongBoundEphemeralSize.class,
    TestMySQLLLSingleStorageQueueLongEphemeralDisabled.class,

    TestMySQLQueue.class,
    TestMySQLQueueLong.class,
    TestMySQLQueueLongBoundEphemeralSize.class,
    TestMySQLQueueLongEphemeralDisabled.class,

    TestMySQLQueueMT.class,
    TestMySQLQueueMTBoundEphemeralSize.class,
    TestMySQLQueueMTEphemeralDisabled.class,

    TestMySQLSingleStorageQueue.class,
    TestMySQLSingleStorageQueueLong.class,
    TestMySQLSingleStorageQueueLongBoundEphemeralSize.class,
    TestMySQLSingleStorageQueueLongEphemeralDisabled.class,

    TestMySQLSingleStorageQueueMT.class,
    TestMySQLSingleStorageQueueMTBoundEphemeralSize.class,
    TestMySQLSingleStorageQueueMTEphemeralDisabled.class
})

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.mysql.MySuiteTest -DenableTestsMySql=true
 */

public class MySuiteTest {
}

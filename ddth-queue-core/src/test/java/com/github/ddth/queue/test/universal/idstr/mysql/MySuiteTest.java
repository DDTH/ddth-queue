package com.github.ddth.queue.test.universal.idstr.mysql;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ 
    TestMySQLLLQueue.class,
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
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.mysql.MySuiteTest -DenableTestsMySql=true
 */

public class MySuiteTest {
}

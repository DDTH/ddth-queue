package com.github.ddth.queue.test.universal.idint.rocksdb;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ 
    TestRocksDbQueue.class,
    TestRocksDbQueueLong.class,
    TestRocksDbQueueLongEphemeralDisabled.class,
    TestRocksDbQueueMT.class,
    TestRocksDbQueueMTEphemeralDisabled.class
})

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.rocksdb.MySuiteTest -DenableTestsRocksDb=true
 */

public class MySuiteTest {
}

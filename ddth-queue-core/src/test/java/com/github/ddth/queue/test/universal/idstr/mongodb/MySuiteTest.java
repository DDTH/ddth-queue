package com.github.ddth.queue.test.universal.idstr.mongodb;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ 
    TestMongodbQueue.class,
    TestMongodbQueueLong.class,
    TestMongodbQueueLongBoundEphemeralSize.class,
    TestMongodbQueueLongEphemeralDisabled.class,
    TestMongodbQueueMT.class,
    TestMongodbQueueMTBoundEphemeralSize.class,
    TestMongodbQueueMTEphemeralDisabled.class
})

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.mongodb.MySuiteTest -DenableTestsMongo=true
 */

public class MySuiteTest {
}

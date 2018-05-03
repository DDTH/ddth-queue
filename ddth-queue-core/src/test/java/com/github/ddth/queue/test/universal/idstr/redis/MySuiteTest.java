package com.github.ddth.queue.test.universal.idstr.redis;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ 
    TestRedisQueue.class,
    TestRedisQueueLong.class,
    TestRedisQueueLongBoundEphemeralSize.class,
    TestRedisQueueLongEphemeralDisabled.class,
    TestRedisQueueMT.class,
    TestRedisQueueMTBoundEphemeralSize.class,
    TestRedisQueueMTEphemeralDisabled.class
})

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.redis.MySuiteTest -DenableTestsRedis=true
 */

public class MySuiteTest {
}

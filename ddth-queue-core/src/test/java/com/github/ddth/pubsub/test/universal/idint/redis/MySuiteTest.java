package com.github.ddth.pubsub.test.universal.idint.redis;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ 
    TestRedisPubSubHub.class,
    TestRedisPubSubMT.class 
})

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.pubsub.test.universal.idint.redis.MySuiteTest -DenableTestsRedis=true
 */

public class MySuiteTest {
}

package com.github.ddth.pubsub.test.universal.idstr.redis;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ 
    TestRedisPubSubHub.class,
    TestRedisPubSubMT.class 
})

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.pubsub.test.universal.idstr.redis.MySuiteTest -DenableTestsRedis=true
 */

public class MySuiteTest {
}

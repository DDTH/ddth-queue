package com.github.ddth.pubsub.test.universal.idstr.redis;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.impl.RedisPubSubHub;
import com.github.ddth.pubsub.impl.universal.idstr.UniversalRedisPubSubHub;
import com.github.ddth.pubsub.test.universal.BasePubSubFunctionalTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test pub-sub functionality.
 */
public class TestRedisPubSubHub extends BasePubSubFunctionalTest<String> {
    public TestRedisPubSubHub(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRedisPubSubHub.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected long catchupSleepMs() {
        return 1000;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected IPubSubHub<String, byte[]> initPubSubHubInstance() throws Exception {
        if (System.getProperty("enableTestsRedis") == null) {
            return null;
        }
        String redisHost = System.getProperty("redis.host", "localhost");
        String redisPort = System.getProperty("redis.port", "6379");
        RedisPubSubHub<String, byte[]> hub = new UniversalRedisPubSubHub();
        hub.setRedisHostAndPort(redisHost + ":" + redisPort);
        hub.init();
        while (!hub.isReady()) {
            Thread.sleep(1);
        }
        return hub;
    }

}

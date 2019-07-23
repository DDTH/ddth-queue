package com.github.ddth.queue.test.universal.idstr.redis;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueLongTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.redis.TestRedisQueueLongEphemeralDisabled -DenableTestsRedis=true
 */

public class TestRedisQueueLongEphemeralDisabled extends BaseQueueLongTest<String> {
    public TestRedisQueueLongEphemeralDisabled(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRedisQueueLongEphemeralDisabled.class);
    }

    @Override
    protected IQueue<String, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsRedis") == null) {
            return null;
        }
        String redisHost = System.getProperty("redis.host", "localhost");
        String redisPort = System.getProperty("redis.port", "6379");

        MyQueue queue = new MyQueue();
        queue.setRedisHostAndPort(redisHost + ":" + redisPort).setEphemeralDisabled(true).init();
        queue.flush();
        return queue;
    }

    protected int numTestMessages() {
        // to make a very long queue
        return 16 * 1024;
    }
}

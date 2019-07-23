package com.github.ddth.queue.test.universal.idstr.redis;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.redis.TestRedisQueueMTEphemeralDisabled -DenableTestsRedis=true
 */

public class TestRedisQueueMTEphemeralDisabled extends BaseQueueMultiThreadsTest<String> {
    public TestRedisQueueMTEphemeralDisabled(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRedisQueueMTEphemeralDisabled.class);
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
        return 16 * 1024;
    }
}

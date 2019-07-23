package com.github.ddth.queue.test.universal.idstr.redis;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueFunctionalTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.redis.TestRedisQueue -DenableTestsRedis=true
 */

public class TestRedisQueue extends BaseQueueFunctionalTest<String> {
    public TestRedisQueue(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRedisQueue.class);
    }

    protected IQueue<String, byte[]> initQueueInstance(int ephemeralMaxSize) throws Exception {
        if (System.getProperty("enableTestsRedis") == null) {
            return null;
        }
        String redisHost = System.getProperty("redis.host", "localhost");
        String redisPort = System.getProperty("redis.port", "6379");

        MyQueue queue = new MyQueue();
        queue.setRedisHostAndPort(redisHost + ":" + redisPort).setEphemeralDisabled(false)
                .setEphemeralMaxSize(ephemeralMaxSize).init();
        queue.flush();
        return queue;
    }
}

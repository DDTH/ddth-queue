package com.github.ddth.queue.test.universal.idint.redis;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.universal.idint.UniversalRedisQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;

import junit.framework.Test;
import junit.framework.TestSuite;
import redis.clients.jedis.Jedis;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.redis.TestRedisQueueMTBoundEphemeralSize -DenableTestsRedis=true
 */

public class TestRedisQueueMTBoundEphemeralSize extends BaseQueueMultiThreadsTest<Long> {
    public TestRedisQueueMTBoundEphemeralSize(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRedisQueueMTBoundEphemeralSize.class);
    }

    private static class MyRedisQueue extends UniversalRedisQueue {
        public void flush() {
            try (Jedis jedis = getJedisPool().getResource()) {
                jedis.flushAll();
            }
        }
    }

    @Override
    protected IQueue<Long, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsRedis") == null) {
            return null;
        }
        String redisHost = System.getProperty("redis.host", "localhost");
        String redisPort = System.getProperty("redis.port", "6379");

        MyRedisQueue queue = new MyRedisQueue();
        queue.setRedisHostAndPort(redisHost + ":" + redisPort).setEphemeralDisabled(false)
                .setEphemeralMaxSize(16).init();
        queue.flush();
        return queue;
    }

    protected int numTestMessages() {
        return 128 * 1024;
    }

}

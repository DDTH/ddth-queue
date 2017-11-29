package com.github.ddth.queue.test.universal;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.RedisQueue;
import com.github.ddth.queue.impl.universal.idint.UniversalRedisQueue;

import junit.framework.Test;
import junit.framework.TestSuite;
import redis.clients.jedis.Jedis;

public class TestRedisQueueMTBoundEphemeralSize extends BaseQueueMultiThreadsTest {
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
    protected IQueue initQueueInstance() throws Exception {
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

    @Override
    protected void destroyQueueInstance(IQueue queue) {
        if (queue instanceof RedisQueue) {
            ((RedisQueue) queue).destroy();
        } else {
            throw new RuntimeException("[queue] is not closed!");
        }
    }

    protected int numTestMessages() {
        return 64 * 1024;
    }

}

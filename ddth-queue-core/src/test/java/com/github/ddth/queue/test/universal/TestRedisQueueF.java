package com.github.ddth.queue.test.universal;

import com.github.ddth.commons.utils.IdGenerator;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.RedisQueue;
import com.github.ddth.queue.impl.universal.UniversalQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalRedisQueue;

import junit.framework.Test;
import junit.framework.TestSuite;
import redis.clients.jedis.Jedis;

public class TestRedisQueueF extends BaseTest {
    public TestRedisQueueF(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRedisQueueF.class);
    }

    private static class MyRedisQueue extends UniversalRedisQueue {
        public void flush() {
            try (Jedis jedis = getJedisPool().getResource()) {
                jedis.flushAll();
            }
        }
    }

    @Override
    protected IQueue initQueueInstance() {
        MyRedisQueue queue = new MyRedisQueue();
        queue.setRedisHostAndPort("localhost:6379").init();
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

    /*----------------------------------------------------------------------*/
    @org.junit.Test
    public void test1() throws Exception {
        assertNull(queue.take());
        assertEquals(0, queue.queueSize());
        assertEquals(0, queue.ephemeralSize());
    }

    @org.junit.Test
    public void test2() throws Exception {
        IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
        String content = idGen.generateId128Ascii();
        UniversalQueueMessage msg = UniversalQueueMessage.newInstance();
        msg.content(content);

        assertTrue(queue.queue(msg));
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.ephemeralSize());
    }

    @org.junit.Test
    public void test3() throws Exception {
        IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
        String content = idGen.generateId128Ascii();
        UniversalQueueMessage msg1 = UniversalQueueMessage.newInstance();
        msg1.content(content);

        assertTrue(queue.queue(msg1));
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.ephemeralSize());

        UniversalQueueMessage msg2 = (UniversalQueueMessage) queue.take();
        assertNotNull(msg2);
        assertEquals(content, msg2.contentAsString());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.ephemeralSize());
    }

    @org.junit.Test
    public void test4() throws Exception {
        IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
        String content = idGen.generateId128Ascii();
        UniversalQueueMessage msg1 = UniversalQueueMessage.newInstance();
        msg1.content(content);

        assertTrue(queue.queue(msg1));
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.ephemeralSize());

        UniversalQueueMessage msg2 = (UniversalQueueMessage) queue.take();
        assertNotNull(msg2);
        assertEquals(content, msg2.contentAsString());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.ephemeralSize());

        queue.finish(msg2);
        assertNull(queue.take());
        assertEquals(0, queue.queueSize());
        assertEquals(0, queue.ephemeralSize());
    }
}

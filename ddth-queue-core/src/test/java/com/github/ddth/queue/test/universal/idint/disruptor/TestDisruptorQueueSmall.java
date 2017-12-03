package com.github.ddth.queue.test.universal.idint.disruptor;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.DisruptorQueue;
import com.github.ddth.queue.impl.universal.idint.UniversalDisruptorQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Tests queue with small ring size.
 */
public class TestDisruptorQueueSmall extends BaseQueueMultiThreadsTest<Long> {
    public TestDisruptorQueueSmall(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDisruptorQueueSmall.class);
    }

    @Override
    protected IQueue<Long, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("skipTestsDisruptor") != null) {
            return null;
        }
        int ringSize = 128;
        DisruptorQueue<Long, byte[]> queue = new UniversalDisruptorQueue();
        queue.setRingSize(ringSize).setEphemeralDisabled(false).init();
        return queue;
    }

}

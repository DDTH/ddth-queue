package com.github.ddth.queue.test.universal.idint.disruptor;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.DisruptorQueue;
import com.github.ddth.queue.impl.universal.idint.UniversalDisruptorQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Tests queue with small ring size, ephemeralDisabled=true.
 */
public class TestDisruptorQueueXSmallEphemeralDisabled extends BaseQueueMultiThreadsTest<Long> {
    public TestDisruptorQueueXSmallEphemeralDisabled(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDisruptorQueueXSmallEphemeralDisabled.class);
    }

    @Override
    protected IQueue<Long, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("skipTestsDisruptor") != null) {
            return null;
        }
        int ringSize = 16;
        DisruptorQueue<Long, byte[]> queue = new UniversalDisruptorQueue();
        queue.setRingSize(ringSize).setEphemeralDisabled(true).init();
        return queue;
    }

}

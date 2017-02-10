package com.github.ddth.queue.test.universal;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.DisruptorQueue;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Tests queue with large ring size, ephemeralDisabled=true.
 */
public class TestDisruptorQueueLargeEphemeralDisabled extends BaseQueueMultiThreadsTest {
    public TestDisruptorQueueLargeEphemeralDisabled(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDisruptorQueueLargeEphemeralDisabled.class);
    }

    @Override
    protected IQueue initQueueInstance() throws Exception {
        if (System.getProperty("skipTestsDisruptor") != null) {
            return null;
        }
        DisruptorQueue queue = new DisruptorQueue();
        queue.setRingSize(8192).setEphemeralDisabled(true).init();
        return queue;
    }

    @Override
    protected void destroyQueueInstance(IQueue queue) {
        if (queue instanceof DisruptorQueue) {
            ((DisruptorQueue) queue).destroy();
        } else {
            throw new RuntimeException("[queue] is not closed!");
        }
    }

}

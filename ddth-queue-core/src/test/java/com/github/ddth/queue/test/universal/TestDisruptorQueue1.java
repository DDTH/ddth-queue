package com.github.ddth.queue.test.universal;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.DisruptorQueue;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test queue functionality.
 */
public class TestDisruptorQueue1 extends BaseQueueFunctionalTest {
    public TestDisruptorQueue1(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDisruptorQueue1.class);
    }

    protected IQueue initQueueInstance(int ephemeralMaxSize) throws Exception {
        if (System.getProperty("skipTestsDisruptor") != null) {
            return null;
        }
        DisruptorQueue queue = new DisruptorQueue();
        queue.setEphemeralDisabled(false).setEphemeralMaxSize(ephemeralMaxSize).init();
        queue.init();
        return queue;
    }

    protected void destroyQueueInstance(IQueue queue) {
        if (queue instanceof DisruptorQueue) {
            ((DisruptorQueue) queue).destroy();
        } else {
            throw new RuntimeException("[queue] is not closed!");
        }
    }

}

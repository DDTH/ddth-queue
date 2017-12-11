package com.github.ddth.queue.test.universal.idstr.disruptor;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.DisruptorQueue;
import com.github.ddth.queue.impl.universal.idstr.UniversalDisruptorQueue;
import com.github.ddth.queue.test.universal.BaseQueueFunctionalTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test queue functionality.
 */
public class TestDisruptorQueue extends BaseQueueFunctionalTest<String> {
    public TestDisruptorQueue(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestDisruptorQueue.class);
    }

    protected IQueue<String, byte[]> initQueueInstance(int ephemeralMaxSize) throws Exception {
        if (System.getProperty("skipTestsDisruptor") != null) {
            return null;
        }
        DisruptorQueue<String, byte[]> queue = new UniversalDisruptorQueue();
        queue.setEphemeralDisabled(false).setEphemeralMaxSize(ephemeralMaxSize).init();
        return queue;
    }

}

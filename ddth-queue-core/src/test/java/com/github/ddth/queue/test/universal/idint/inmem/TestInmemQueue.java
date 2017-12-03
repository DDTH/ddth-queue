package com.github.ddth.queue.test.universal.idint.inmem;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.InmemQueue;
import com.github.ddth.queue.impl.universal.idint.UniversalInmemQueue;
import com.github.ddth.queue.test.universal.BaseQueueFunctionalTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test queue functionality.
 */
public class TestInmemQueue extends BaseQueueFunctionalTest<Long> {
    public TestInmemQueue(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestInmemQueue.class);
    }

    protected IQueue<Long, byte[]> initQueueInstance(int ephemeralMaxSize) throws Exception {
        if (System.getProperty("skipTestsInmem") != null) {
            return null;
        }
        InmemQueue<Long, byte[]> queue = new UniversalInmemQueue();
        queue.setEphemeralDisabled(false).setEphemeralMaxSize(ephemeralMaxSize).init();
        return queue;
    }

}

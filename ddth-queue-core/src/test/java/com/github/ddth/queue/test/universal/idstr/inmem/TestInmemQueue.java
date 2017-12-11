package com.github.ddth.queue.test.universal.idstr.inmem;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.InmemQueue;
import com.github.ddth.queue.impl.universal.idstr.UniversalInmemQueue;
import com.github.ddth.queue.test.universal.BaseQueueFunctionalTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test queue functionality.
 */
public class TestInmemQueue extends BaseQueueFunctionalTest<String> {
    public TestInmemQueue(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestInmemQueue.class);
    }

    protected IQueue<String, byte[]> initQueueInstance(int ephemeralMaxSize) throws Exception {
        if (System.getProperty("skipTestsInmem") != null) {
            return null;
        }
        InmemQueue<String, byte[]> queue = new UniversalInmemQueue();
        queue.setEphemeralDisabled(false).setEphemeralMaxSize(ephemeralMaxSize).init();
        return queue;
    }

}

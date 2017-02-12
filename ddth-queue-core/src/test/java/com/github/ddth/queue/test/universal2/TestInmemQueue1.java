package com.github.ddth.queue.test.universal2;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.InmemQueue;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test queue functionality.
 */
public class TestInmemQueue1 extends BaseQueueFunctionalTest {
    public TestInmemQueue1(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestInmemQueue1.class);
    }

    protected IQueue initQueueInstance(int ephemeralMaxSize) throws Exception {
        if (System.getProperty("skipTestsInmem") != null) {
            return null;
        }
        InmemQueue queue = new InmemQueue();
        queue.setEphemeralDisabled(false).setEphemeralMaxSize(ephemeralMaxSize).init();
        return queue;
    }

    protected void destroyQueueInstance(IQueue queue) {
        if (queue instanceof InmemQueue) {
            ((InmemQueue) queue).destroy();
        } else {
            throw new RuntimeException("[queue] is not closed!");
        }
    }

}

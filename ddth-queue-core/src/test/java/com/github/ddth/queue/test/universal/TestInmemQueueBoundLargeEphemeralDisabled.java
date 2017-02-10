package com.github.ddth.queue.test.universal;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.InmemQueue;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Tests bounded queue with large boundary, ephemeralDisabled=true.
 */
public class TestInmemQueueBoundLargeEphemeralDisabled extends BaseQueueMultiThreadsTest {
    public TestInmemQueueBoundLargeEphemeralDisabled(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestInmemQueueBoundLargeEphemeralDisabled.class);
    }

    @Override
    protected IQueue initQueueInstance() throws Exception {
        if (System.getProperty("skipTestsInmem") != null) {
            return null;
        }
        InmemQueue queue = new InmemQueue();
        queue.setBoundary(8192).setEphemeralDisabled(true).init();
        return queue;
    }

    @Override
    protected void destroyQueueInstance(IQueue queue) {
        if (queue instanceof InmemQueue) {
            ((InmemQueue) queue).destroy();
        } else {
            throw new RuntimeException("[queue] is not closed!");
        }
    }

}

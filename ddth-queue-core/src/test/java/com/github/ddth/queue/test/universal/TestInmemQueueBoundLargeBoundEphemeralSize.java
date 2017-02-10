package com.github.ddth.queue.test.universal;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.InmemQueue;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Tests bounded queue with large boundary.
 */
public class TestInmemQueueBoundLargeBoundEphemeralSize extends BaseQueueMultiThreadsTest {
    public TestInmemQueueBoundLargeBoundEphemeralSize(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestInmemQueueBoundLargeBoundEphemeralSize.class);
    }

    @Override
    protected IQueue initQueueInstance() throws Exception {
        if (System.getProperty("skipTestsInmem") != null) {
            return null;
        }
        InmemQueue queue = new InmemQueue();
        queue.setBoundary(8192).setEphemeralDisabled(false).setEphemeralMaxSize(8192 / 8).init();
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

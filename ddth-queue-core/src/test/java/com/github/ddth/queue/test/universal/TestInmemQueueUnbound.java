package com.github.ddth.queue.test.universal;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.InmemQueue;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Tests unbounded queue.
 */
public class TestInmemQueueUnbound extends BaseQueueMultiThreadsTest {
    public TestInmemQueueUnbound(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestInmemQueueUnbound.class);
    }

    @Override
    protected IQueue initQueueInstance() throws Exception {
        if (System.getProperty("skipTestsInmem") != null) {
            return null;
        }
        InmemQueue queue = new InmemQueue();
        queue.setBoundary(-1).setEphemeralDisabled(false).init();
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

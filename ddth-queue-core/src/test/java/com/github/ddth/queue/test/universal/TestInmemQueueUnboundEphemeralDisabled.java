package com.github.ddth.queue.test.universal;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.InmemQueue;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Tests unbounded queue, ephemeralDisabled=true.
 */
public class TestInmemQueueUnboundEphemeralDisabled extends BaseQueueMultiThreadsTest {
    public TestInmemQueueUnboundEphemeralDisabled(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestInmemQueueUnboundEphemeralDisabled.class);
    }

    @Override
    protected IQueue initQueueInstance() throws Exception {
        if (System.getProperty("skipTestsInmem") != null) {
            return null;
        }
        InmemQueue queue = new InmemQueue();
        queue.setBoundary(-1).setEphemeralDisabled(true).init();
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

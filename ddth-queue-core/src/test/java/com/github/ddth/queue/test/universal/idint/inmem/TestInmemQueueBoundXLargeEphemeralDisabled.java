package com.github.ddth.queue.test.universal.idint.inmem;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.InmemQueue;
import com.github.ddth.queue.impl.universal.idint.UniversalInmemQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Tests bounded queue with large boundary, ephemeralDisabled=true.
 */
public class TestInmemQueueBoundXLargeEphemeralDisabled extends BaseQueueMultiThreadsTest<Long> {
    public TestInmemQueueBoundXLargeEphemeralDisabled(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestInmemQueueBoundXLargeEphemeralDisabled.class);
    }

    @Override
    protected IQueue<Long, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("skipTestsInmem") != null) {
            return null;
        }
        int size = 8192 * 8;
        InmemQueue<Long, byte[]> queue = new UniversalInmemQueue();
        queue.setBoundary(size).setEphemeralDisabled(true).init();
        return queue;
    }

}

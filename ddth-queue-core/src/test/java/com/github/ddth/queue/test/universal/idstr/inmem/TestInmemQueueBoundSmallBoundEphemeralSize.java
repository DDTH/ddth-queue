package com.github.ddth.queue.test.universal.idstr.inmem;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.InmemQueue;
import com.github.ddth.queue.impl.universal.idstr.UniversalInmemQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Tests bounded queue with small boundary.
 */
public class TestInmemQueueBoundSmallBoundEphemeralSize extends BaseQueueMultiThreadsTest<String> {
    public TestInmemQueueBoundSmallBoundEphemeralSize(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestInmemQueueBoundSmallBoundEphemeralSize.class);
    }

    @Override
    protected IQueue<String, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("skipTestsInmem") != null) {
            return null;
        }
        int size = 128;
        InmemQueue<String, byte[]> queue = new UniversalInmemQueue();
        queue.setBoundary(size).setEphemeralDisabled(false).setEphemeralMaxSize(size / 8).init();
        return queue;
    }

}

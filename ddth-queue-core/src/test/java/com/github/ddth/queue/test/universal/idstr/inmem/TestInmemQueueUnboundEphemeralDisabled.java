package com.github.ddth.queue.test.universal.idstr.inmem;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.InmemQueue;
import com.github.ddth.queue.impl.universal.idstr.UniversalInmemQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Tests unbounded queue, ephemeralDisabled=true.
 */
public class TestInmemQueueUnboundEphemeralDisabled extends BaseQueueMultiThreadsTest<String> {
    public TestInmemQueueUnboundEphemeralDisabled(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestInmemQueueUnboundEphemeralDisabled.class);
    }

    @Override
    protected IQueue<String, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("skipTestsInmem") != null) {
            return null;
        }
        InmemQueue<String, byte[]> queue = new UniversalInmemQueue();
        queue.setBoundary(-1).setEphemeralDisabled(true).init();
        return queue;
    }

}

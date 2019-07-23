package com.github.ddth.pubsub.test.universal.idint.inmem;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.impl.universal.idint.UniversalInmemPubSubHub;
import com.github.ddth.pubsub.test.universal.BasePubSubMultiThreadsTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test pub-sub functionality, multi-threads.
 */
public class TestInmemPubSubMT extends BasePubSubMultiThreadsTest<Long> {
    public TestInmemPubSubMT(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestInmemPubSubMT.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected IPubSubHub<Long, byte[]> initPubSubHubInstance() {
        if (System.getProperty("skipTestsInmem") != null) {
            return null;
        }
        return new UniversalInmemPubSubHub().init();
    }
}

package com.github.ddth.pubsub.test.universal.idstr.inmem;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.impl.InmemPubSubHub;
import com.github.ddth.pubsub.impl.universal.idstr.UniversalInmemPubSubHub;
import com.github.ddth.pubsub.test.universal.BasePubSubMultiThreadsTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test pub-sub functionality, multi-threads.
 */
public class TestInmemPubSubMT extends BasePubSubMultiThreadsTest<String> {
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
    protected IPubSubHub<String, byte[]> initPubSubHubInstance() {
        if (System.getProperty("skipTestsInmem") != null) {
            return null;
        }
        InmemPubSubHub<String, byte[]> hub = new UniversalInmemPubSubHub();
        hub.init();
        return hub;
    }
}

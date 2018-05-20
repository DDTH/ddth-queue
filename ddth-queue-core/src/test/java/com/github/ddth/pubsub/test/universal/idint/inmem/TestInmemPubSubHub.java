package com.github.ddth.pubsub.test.universal.idint.inmem;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.impl.InmemPubSubHub;
import com.github.ddth.pubsub.impl.universal.idint.UniversalInmemPubSubHub;
import com.github.ddth.pubsub.test.universal.BasePubSubFunctionalTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test pub-sub functionality.
 */
public class TestInmemPubSubHub extends BasePubSubFunctionalTest<Long> {
    public TestInmemPubSubHub(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestInmemPubSubHub.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected IPubSubHub<Long, byte[]> initPubSubHubInstance() throws Exception {
        if (System.getProperty("skipTestsInmem") != null) {
            return null;
        }
        InmemPubSubHub<Long, byte[]> hub = new UniversalInmemPubSubHub();
        hub.init();
        return hub;
    }

}

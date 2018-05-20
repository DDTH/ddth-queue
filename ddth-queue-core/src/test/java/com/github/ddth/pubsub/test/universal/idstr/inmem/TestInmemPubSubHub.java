package com.github.ddth.pubsub.test.universal.idstr.inmem;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.impl.InmemPubSubHub;
import com.github.ddth.pubsub.impl.universal.idstr.UniversalInmemPubSubHub;
import com.github.ddth.pubsub.test.universal.BasePubSubFunctionalTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test pub-sub functionality.
 */
public class TestInmemPubSubHub extends BasePubSubFunctionalTest<String> {
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
    protected IPubSubHub<String, byte[]> initPubSubHubInstance() throws Exception {
        if (System.getProperty("skipTestsInmem") != null) {
            return null;
        }
        InmemPubSubHub<String, byte[]> hub = new UniversalInmemPubSubHub();
        hub.init();
        return hub;
    }

}

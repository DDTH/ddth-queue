package com.github.ddth.pubsub.test.universal.idint.mongodb;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.impl.MongodbPubSubHub;
import com.github.ddth.pubsub.impl.universal.idint.UniversalMongodbPubSubHub;
import com.github.ddth.pubsub.test.universal.BasePubSubFunctionalTest;
import com.github.ddth.queue.utils.MongoUtils;

import junit.framework.Test;
import junit.framework.TestSuite;

/**
 * Test pub-sub functionality.
 */
public class TestMongodbPubSubHub extends BasePubSubFunctionalTest<Long> {
    public TestMongodbPubSubHub(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestMongodbPubSubHub.class);
    }

    private static class MyMongodbPubSubHub extends UniversalMongodbPubSubHub {
        public MyMongodbPubSubHub init() {
            super.init();
            MongoUtils.dropCollection(getDatabase(), "demo");
            MongoUtils.dropCollection(getDatabase(), "demo0");
            MongoUtils.dropCollection(getDatabase(), "demo1");
            MongoUtils.dropCollection(getDatabase(), "demo2");
            MongoUtils.dropCollection(getDatabase(), "demo3");
            MongoUtils.dropCollection(getDatabase(), "demo4");
            MongoUtils.dropCollection(getDatabase(), "demo5");
            MongoUtils.dropCollection(getDatabase(), "demo6");
            MongoUtils.dropCollection(getDatabase(), "demo7");
            MongoUtils.dropCollection(getDatabase(), "demo8");
            MongoUtils.dropCollection(getDatabase(), "demo9");
            return this;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected long catchupSleepMs() {
        return 1000;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected IPubSubHub<Long, byte[]> initPubSubHubInstance() throws Exception {
        if (System.getProperty("enableTestsMongo") == null
                && System.getProperty("enableTestsMongoDB") == null
                && System.getProperty("enableTestsMongoDb") == null
                && System.getProperty("enableTestsMongodb") == null) {
            return null;
        }
        String mongoUri = System.getProperty("mongo.uri",
                "mongodb://test:test@localhost:27017/test");
        String mongoDb = System.getProperty("mongo.db", "test");

        MongodbPubSubHub<Long, byte[]> hub = new MyMongodbPubSubHub();
        hub.setConnectionString(mongoUri).setDatabaseName(mongoDb);
        hub.init();
        return hub;
    }

}

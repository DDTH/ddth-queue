package com.github.ddth.queue.test.universal.idstr.mongodb;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.universal.idstr.UniversalMongodbQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.mongodb.TestMongodbQueueMTEphemeralDisabled -DenableTestsMongo=true
 */

public class TestMongodbQueueMTEphemeralDisabled extends BaseQueueMultiThreadsTest<String> {
    public TestMongodbQueueMTEphemeralDisabled(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestMongodbQueueMTEphemeralDisabled.class);
    }

    private static class MyMongoQueue extends UniversalMongodbQueue {
        public void flush() {
            getCollection().drop();
            initCollection();
        }
    }

    @Override
    protected IQueue<String, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsMongo") == null
                && System.getProperty("enableTestsMongoDB") == null
                && System.getProperty("enableTestsMongoDb") == null
                && System.getProperty("enableTestsMongodb") == null) {
            return null;
        }
        String mongoUri = System.getProperty("mongo.uri",
                "mongodb://test:test@localhost:27017/test");
        String mongoDb = System.getProperty("mongo.db", "test");
        String mongoCollection = System.getProperty("mongo.collection", "ddth_queue");

        MyMongoQueue queue = new MyMongoQueue();
        queue.setCollectionName(mongoCollection).setDatabaseName(mongoDb)
                .setConnectionString(mongoUri).setEphemeralDisabled(true).init();
        queue.flush();
        return queue;
    }

    protected int numTestMessages() {
        // to make a very long queue
        return 8 * 1024;
    }
}

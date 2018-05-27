package com.github.ddth.queue.test.universal.idint.mongodb;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.universal.idint.UniversalMongodbQueue;
import com.github.ddth.queue.test.universal.BaseQueueFunctionalTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.mongodb.TestMongodbQueue -DenableTestsMongo=true
 */

public class TestMongodbQueue extends BaseQueueFunctionalTest<Long> {
    public TestMongodbQueue(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestMongodbQueue.class);
    }

    private static class MyMongoQueue extends UniversalMongodbQueue {
        public void flush() {
            getCollection().drop();
            initCollection();
        }
    }

    protected IQueue<Long, byte[]> initQueueInstance(int ephemeralMaxSize) throws Exception {
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
                .setConnectionString(mongoUri).setEphemeralDisabled(false)
                .setEphemeralMaxSize(ephemeralMaxSize).init();
        queue.flush();
        return queue;
    }

}

package com.github.ddth.queue.test.universal.idint.mongodb;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueLongTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.mongodb.TestMongodbQueueLong -DenableTestsMongo=true
 */

public class TestMongodbQueueLong extends BaseQueueLongTest<Long> {
    public TestMongodbQueueLong(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestMongodbQueueLong.class);
    }

    @Override
    protected IQueue<Long, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsMongo") == null && System.getProperty("enableTestsMongoDB") == null
                && System.getProperty("enableTestsMongoDb") == null
                && System.getProperty("enableTestsMongodb") == null) {
            return null;
        }
        String mongoUri = System.getProperty("mongo.uri", "mongodb://test:test@localhost:27017/test");
        String mongoDb = System.getProperty("mongo.db", "test");
        String mongoCollection = System.getProperty("mongo.collection", "ddth_queue");

        MyQueue queue = new MyQueue();
        queue.setCollectionName(mongoCollection).setDatabaseName(mongoDb).setConnectionString(mongoUri)
                .setEphemeralDisabled(false).init();
        queue.flush();
        return queue;
    }

    protected int numTestMessages() {
        // to make a very long queue
        return 8 * 1024;
    }
}

package com.github.ddth.queue.test.universal.idstr.activemq;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueLongTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.activemq.TestActiveMqQueueLong -DenableTestsActiveMq=true
 */

public class TestActiveMqQueueLong extends BaseQueueLongTest<String> {
    public TestActiveMqQueueLong(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestActiveMqQueueLong.class);
    }

    protected IQueue<String, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsActiveMq") == null) {
            return null;
        }
        String uri = System.getProperty("activemq.uri", "tcp://localhost:61616");
        String queueName = System.getProperty("activemq.queue", "ddth-queue");

        MyQueue queue = new MyQueue();
        queue.setUri(uri).setQueueName(queueName).init();
        queue.flush();
        return queue;
    }

    protected int numTestMessages() {
        // to make a very long queue
        return 16 * 1024;
    }
}

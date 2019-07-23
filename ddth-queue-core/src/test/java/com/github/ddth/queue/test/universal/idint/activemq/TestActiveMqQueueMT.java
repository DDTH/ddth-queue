package com.github.ddth.queue.test.universal.idint.activemq;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.activemq.TestActiveMqQueueMT -DenableTestsActiveMq=true
 */

public class TestActiveMqQueueMT extends BaseQueueMultiThreadsTest<Long> {
    public TestActiveMqQueueMT(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestActiveMqQueueMT.class);
    }

    protected IQueue<Long, byte[]> initQueueInstance() throws Exception {
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
        return 8 * 1024;
    }
}

package com.github.ddth.queue.test.universal.idint.rabbitmq;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.rabbitmq.TestRabbitMqQueueMT -DenableTestsRabbitMq=true
 */

public class TestRabbitMqQueueMT extends BaseQueueMultiThreadsTest<Long> {
    public TestRabbitMqQueueMT(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRabbitMqQueueMT.class);
    }

    protected IQueue<Long, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsRabbitMq") == null) {
            return null;
        }
        String uri = System.getProperty("rabbitmq.uri", "amqp://localhost:5672");
        String queueName = System.getProperty("rabbitmq.queue", "ddth-queue");

        MyQueue queue = new MyQueue();
        queue.setUri(uri).setQueueName(queueName).init();
        queue.flush();
        return queue;
    }

    protected int numTestMessages() {
        return 8 * 1024;
    }
}

package com.github.ddth.queue.test.universal.idstr.rabbitmq;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueLongTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.rabbitmq.TestRabbitMqQueueLong -DenableTestsRabbitMq=true
 */

public class TestRabbitMqQueueLong extends BaseQueueLongTest<String> {
    public TestRabbitMqQueueLong(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRabbitMqQueueLong.class);
    }

    protected IQueue<String, byte[]> initQueueInstance() throws Exception {
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
        // to make a very long queue
        return 16 * 1024;
    }
}

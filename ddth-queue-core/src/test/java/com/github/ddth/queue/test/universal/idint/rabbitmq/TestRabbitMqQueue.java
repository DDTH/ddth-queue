package com.github.ddth.queue.test.universal.idint.rabbitmq;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueFunctionalTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.rabbitmq.TestRabbitMqQueue -DenableTestsRabbitMq=true
 */

public class TestRabbitMqQueue extends BaseQueueFunctionalTest<Long> {
    public TestRabbitMqQueue(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRabbitMqQueue.class);
    }

    protected IQueue<Long, byte[]> initQueueInstance(int ephemeralMaxSize) throws Exception {
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
}

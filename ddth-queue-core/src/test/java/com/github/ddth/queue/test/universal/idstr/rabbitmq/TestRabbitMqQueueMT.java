package com.github.ddth.queue.test.universal.idstr.rabbitmq;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.idstr.UniversalRabbitMqQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.rabbitmq.TestActiveMqQueueMT -DenableTestsRabbitMq=true
 */

public class TestRabbitMqQueueMT extends BaseQueueMultiThreadsTest<String> {
    public TestRabbitMqQueueMT(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(
                TestRabbitMqQueueMT.class);
    }

    private static class MyRabbitMqQueue extends UniversalRabbitMqQueue {
        public void flush() {
            int numMsgs = 0;
            long t1 = System.currentTimeMillis();
            IQueueMessage<String, byte[]> msg = take();
            while (msg != null) {
                numMsgs++;
                msg = take();
            }
            msg = take();
            System.out.println(
                    "* Flush " + numMsgs + " msgs from queue in " + (System.currentTimeMillis()
                            - t1) + "ms.");
        }
    }

    protected IQueue<String, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsRabbitMq") == null) {
            return null;
        }
        String uri = System.getProperty("rabbitmq.uri", "amqp://localhost:5672");
        String queueName = System.getProperty("rabbitmq.queue", "ddth-queue");

        MyRabbitMqQueue queue = new MyRabbitMqQueue();
        queue.setUri(uri).setQueueName(queueName).init();
        queue.flush();
        return queue;
    }

    protected int numTestMessages() {
        return 8 * 1024;
    }

}

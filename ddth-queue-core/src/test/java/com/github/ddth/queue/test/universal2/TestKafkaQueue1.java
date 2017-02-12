package com.github.ddth.queue.test.universal2;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.KafkaQueue;
import com.github.ddth.queue.impl.universal2.UniversalKafkaQueue;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestKafkaQueue1 extends BaseQueueFunctionalTest {
    public TestKafkaQueue1(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestKafkaQueue1.class);
    }

    private static class MyKafkaQueue extends UniversalKafkaQueue {
        public void flush() {
            IQueueMessage msg = takeFromQueue();
            while (msg != null) {
                msg = takeFromQueue();
            }
            msg = takeFromQueue();
        }
    }

    protected IQueue initQueueInstance(int ephemeralMaxSize) throws Exception {
        if (System.getProperty("enableTestsKafka") == null) {
            return null;
        }
        String kafkaBrokers = System.getProperty("kafka.brokers", "localhost:9092");
        String topic = System.getProperty("kafka.topic", "ddth-queue");
        String groupId = System.getProperty("kafka.groupid", "ddth-queue");

        MyKafkaQueue queue = new MyKafkaQueue();
        queue.setKafkaBootstrapServers(kafkaBrokers).setTopicName(topic).setConsumerGroupId(groupId)
                .setSendAsync(false).init();
        queue.flush();
        return queue;
    }

    protected void destroyQueueInstance(IQueue queue) {
        if (queue instanceof KafkaQueue) {
            ((KafkaQueue) queue).destroy();
        } else {
            throw new RuntimeException("[queue] is not closed!");
        }
    }

}

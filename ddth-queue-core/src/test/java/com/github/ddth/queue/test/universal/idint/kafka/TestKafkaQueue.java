package com.github.ddth.queue.test.universal.idint.kafka;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueFunctionalTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.kafka.TestKafkaQueue -DenableTestsKafka=true
 */

public class TestKafkaQueue extends BaseQueueFunctionalTest<Long> {
    public TestKafkaQueue(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestKafkaQueue.class);
    }

    protected IQueue<Long, byte[]> initQueueInstance(int ephemeralMaxSize) throws Exception {
        if (System.getProperty("enableTestsKafka") == null) {
            return null;
        }
        String kafkaBrokers = System.getProperty("kafka.brokers", "localhost:9092");
        String topic = System.getProperty("kafka.topic", "ddth-queue");
        String groupId = System.getProperty("kafka.groupid", "ddth-queue");

        MyQueue queue = new MyQueue();
        queue.setKafkaBootstrapServers(kafkaBrokers).setTopicName(topic).setConsumerGroupId(groupId).setSendAsync(false)
                .init();
        queue.flush();
        return queue;
    }
}

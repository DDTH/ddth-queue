package com.github.ddth.queue.test.universal.idstr.kafka;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueLongTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.kafka.TestKafkaQueueLong -DenableTestsKafka=true
 */

public class TestKafkaQueueLong extends BaseQueueLongTest<String> {
    public TestKafkaQueueLong(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestKafkaQueueLong.class);
    }

    protected IQueue<String, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsKafka") == null) {
            return null;
        }
        String kafkaBrokers = System.getProperty("kafka.brokers", "localhost:9092");
        String topic = System.getProperty("kafka.topic", "ddth-queue");
        String groupId = System.getProperty("kafka.groupid", "ddth-queue");

        MyQueue queue = new MyQueue();
        queue.setKafkaBootstrapServers(kafkaBrokers).setTopicName(topic).setConsumerGroupId(groupId).setSendAsync(true)
                .init();
        queue.flush();
        return queue;
    }

    protected int numTestMessages() {
        // to make a very long queue
        return 16 * 1024;
    }
}

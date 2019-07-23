package com.github.ddth.queue.test.universal.idint.kafka;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.kafka.TestKafkaQueueMT -DenableTestsKafka=true
 */

public class TestKafkaQueueMT extends BaseQueueMultiThreadsTest<Long> {
    public TestKafkaQueueMT(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestKafkaQueueMT.class);
    }

    protected IQueue<Long, byte[]> initQueueInstance() throws Exception {
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
        return 16 * 1024;
    }
}

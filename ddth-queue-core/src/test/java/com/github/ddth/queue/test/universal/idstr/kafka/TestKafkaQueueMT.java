package com.github.ddth.queue.test.universal.idstr.kafka;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.idstr.UniversalKafkaQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.kafka.TestActiveMqQueueMT -DenableTestsKafka=true
 */

public class TestKafkaQueueMT extends BaseQueueMultiThreadsTest<String> {
    public TestKafkaQueueMT(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestKafkaQueueMT.class);
    }

    private static class MyKafkaQueue extends UniversalKafkaQueue {
        public void flush() {
            int numMsgs = 0;
            long t1 = System.currentTimeMillis();
            IQueueMessage<String, byte[]> msg = takeFromQueue();
            while (msg != null) {
                numMsgs++;
                msg = takeFromQueue();
            }
            msg = takeFromQueue();
            System.out.println("* Flush " + numMsgs + " msgs from queue in "
                    + (System.currentTimeMillis() - t1) + "ms.");
        }
    }

    protected IQueue<String, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsKafka") == null) {
            return null;
        }
        String kafkaBrokers = System.getProperty("kafka.brokers", "localhost:9092");
        String topic = System.getProperty("kafka.topic", "ddth-queue");
        String groupId = System.getProperty("kafka.groupid", "ddth-queue");

        MyKafkaQueue queue = new MyKafkaQueue();
        queue.setKafkaBootstrapServers(kafkaBrokers).setTopicName(topic).setConsumerGroupId(groupId)
                .setSendAsync(true).init();
        queue.flush();
        return queue;
    }

    protected int numTestMessages() {
        return 16 * 1024;
    }

}

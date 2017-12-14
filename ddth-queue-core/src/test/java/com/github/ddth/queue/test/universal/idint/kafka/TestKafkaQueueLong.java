package com.github.ddth.queue.test.universal.idint.kafka;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalKafkaQueue;
import com.github.ddth.queue.test.universal.BaseQueueLongTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.kafka.TestActiveMqQueueLong -DenableTestsKafka=true
 */

public class TestKafkaQueueLong extends BaseQueueLongTest<Long> {
    public TestKafkaQueueLong(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestKafkaQueueLong.class);
    }

    private static class MyKafkaQueue extends UniversalKafkaQueue {
        public void flush() {
            int numMsgs = 0;
            long t1 = System.currentTimeMillis();
            IQueueMessage<Long, byte[]> msg = takeFromQueue();
            while (msg != null) {
                numMsgs++;
                msg = takeFromQueue();
            }
            msg = takeFromQueue();
            System.out.println("* Flush " + numMsgs + " msgs from queue in "
                    + (System.currentTimeMillis() - t1) + "ms.");
        }
    }

    protected IQueue<Long, byte[]> initQueueInstance() throws Exception {
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
        // to make a very long queue
        return 16 * 1024;
    }

}

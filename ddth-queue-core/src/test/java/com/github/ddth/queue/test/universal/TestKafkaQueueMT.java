package com.github.ddth.queue.test.universal;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.KafkaQueue;
import com.github.ddth.queue.impl.universal.idint.UniversalKafkaQueue;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestKafkaQueueMT extends BaseQueueMultiThreadsTest {
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
            IQueueMessage msg = takeFromQueue();
            while (msg != null) {
                numMsgs++;
                msg = takeFromQueue();
            }
            msg = takeFromQueue();
            System.out.println("* Flush " + numMsgs + " msgs from queue in "
                    + (System.currentTimeMillis() - t1) + "ms.");
        }
    }

    protected IQueue initQueueInstance() throws Exception {
        // if (System.getProperty("enableTestsKafka") == null) {
        // return null;
        // }
        String kafkaBrokers = System.getProperty("kafka.brokers", "localhost:9092");
        String topic = System.getProperty("kafka.topic", "ddth-queue");
        String groupId = System.getProperty("kafka.groupid", "ddth-queue");

        MyKafkaQueue queue = new MyKafkaQueue();
        queue.setKafkaBootstrapServers(kafkaBrokers).setTopicName(topic).setConsumerGroupId(groupId)
                .setSendAsync(true).init();
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

    protected int numTestMessages() {
        return 16 * 1024;
    }

}

package com.github.ddth.queue.qnd.universal;

import java.util.Date;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalKafkaQueue;

public class QndQueueKafka {

    private static void emptyQueue(IQueue<Long, byte[]> queue) {
        IQueueMessage<Long, byte[]> msg = queue.take();
        while (msg != null) {
            queue.finish(msg);
            msg = queue.take();
        }
    }

    public static void main(String[] args) throws Exception {
        try (final UniversalKafkaQueue queue = new UniversalKafkaQueue()) {
            queue.setKafkaBootstrapServers("localhost:9092").setTopicName("ddth-queue")
                    .setConsumerGroupId("ddth-queue").init();

            emptyQueue(queue);
            UniversalIdIntQueueMessage msg = queue.take();
            System.out.println("Queue empty: " + msg);

            msg = UniversalIdIntQueueMessage.newInstance();
            msg.content("Content: [" + System.currentTimeMillis() + "] " + new Date());
            System.out.println("Queue: " + queue.queue(msg));

            msg = queue.take();
            while (msg.getNumRequeues() < 2) {
                System.out.println("Message: " + msg);
                System.out.println("Content: " + new String(msg.content()));
                System.out.println("Requeue: " + queue.requeue(msg));
                msg = queue.take();
            }

            queue.finish(msg);
        }
    }
}

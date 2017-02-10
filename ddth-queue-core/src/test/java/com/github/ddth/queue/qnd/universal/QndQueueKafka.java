package com.github.ddth.queue.qnd.universal;

import java.util.Date;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalKafkaQueue;
import com.github.ddth.queue.impl.universal.UniversalQueueMessage;

public class QndQueueKafka {

    private static void emptyQueue(IQueue queue) {
        IQueueMessage msg = queue.take();
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
            UniversalQueueMessage msg = queue.take();
            System.out.println("Queue empty: " + msg);

            msg = UniversalQueueMessage.newInstance();
            msg.content("Content: [" + System.currentTimeMillis() + "] " + new Date());
            System.out.println("Queue: " + queue.queue(msg));

            msg = queue.take();
            while (msg.qNumRequeues() < 2) {
                System.out.println("Message: " + msg);
                System.out.println("Content: " + new String(msg.content()));
                System.out.println("Requeue: " + queue.requeue(msg));
                msg = queue.take();
            }

            queue.finish(msg);
        }
    }
}

package com.github.ddth.queue.qnd.universal;

import java.util.Date;

import com.github.ddth.queue.impl.universal.UniversalKafkaQueue;
import com.github.ddth.queue.impl.universal.UniversalQueueMessage;

public class QueueKafka {

    public static void main(String[] args) throws Exception {
        final UniversalKafkaQueue queue = new UniversalKafkaQueue();
        queue.setZkConnString("localhost:2181/kafka").setTopicName("ddth-queue")
                .setConsumerGroupId("ddth-queue").init();

        UniversalQueueMessage msg = UniversalQueueMessage.newInstance();
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

        queue.destroy();
    }
}

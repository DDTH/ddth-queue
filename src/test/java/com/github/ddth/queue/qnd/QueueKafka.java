package com.github.ddth.queue.qnd;

import java.util.Date;

import com.github.ddth.queue.UniversalQueueMessage;
import com.github.ddth.queue.impl.UniversalKafkaQueue;

public class QueueKafka {

    public static void main(String[] args) throws Exception {
        final UniversalKafkaQueue queue = new UniversalKafkaQueue();
        queue.setZkConnString("localhost:2181/kafka").setTopicName("ddth-queue").init();

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < 1000; i++) {
            UniversalQueueMessage msg = new UniversalQueueMessage();
            String content = "Content: [" + System.currentTimeMillis() + "] " + new Date();
            msg.qNumRequeues(0).qOriginalTimestamp(new Date()).qTimestamp(new Date())
                    .content(content.getBytes());
            System.out.println("Queue: " + queue.queue(msg));
        }
        long t2 = System.currentTimeMillis();
        System.out.println(t2 - t1);
        System.out.println((t2 - t1) / 1000.0);

        // msg = queue.take();
        // while (msg.qNumRequeues() < 2) {
        // System.out.println(msg);
        // System.out.println("Requeue: " + queue.requeue(msg));
        // msg = queue.take();
        // }
        //
        // queue.finish(msg);

        queue.destroy();
    }
}

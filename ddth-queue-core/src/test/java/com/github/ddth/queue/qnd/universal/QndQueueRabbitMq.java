package com.github.ddth.queue.qnd.universal;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalRabbitMqQueue;
import com.github.ddth.queue.impl.universal.msg.UniversalIdIntQueueMessage;

import java.util.Date;

public class QndQueueRabbitMq {

    private static void emptyQueue(IQueue<Long, byte[]> queue) {
        IQueueMessage<Long, byte[]> msg = queue.take();
        while (msg != null) {
            queue.finish(msg);
            msg = queue.take();
        }
    }

    public static void main(String[] args) throws Exception {
        try (final UniversalRabbitMqQueue queue = new UniversalRabbitMqQueue()) {
            queue.setUri("amqp://guest:guest@localhost:5672").setQueueName("ddth-queue").init();

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

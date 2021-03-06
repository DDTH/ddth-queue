package com.github.ddth.queue.qnd.universal;

import java.util.Date;

import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalDisruptorQueue;

public class QndQueueDisruptor {
    public static void main(String[] args) throws Exception {
        try (final UniversalDisruptorQueue queue = new UniversalDisruptorQueue()) {
            queue.init();

            UniversalIdIntQueueMessage msg = UniversalIdIntQueueMessage.newInstance();
            msg.setContent("Content: [" + System.currentTimeMillis() + "] " + new Date());
            System.out.println("Queue: " + queue.queue(msg));

            msg = queue.take();
            while (msg.getNumRequeues() < 2) {
                System.out.println("Message: " + msg);
                System.out.println("Content: " + new String(msg.getContent()));
                System.out.println("Requeue: " + queue.requeue(msg));
                msg = queue.take();
            }

            queue.finish(msg);
        }
    }
}

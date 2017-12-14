package com.github.ddth.queue.qnd.universal;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalActiveMqQueue;
import com.github.ddth.queue.impl.universal.msg.UniversalIdIntQueueMessage;

import java.util.Date;

public class QndQueueActiveMq {

    private static void emptyQueue(IQueue queue) {
        IQueueMessage msg = queue.take();
        while (msg != null) {
            queue.finish(msg);
            msg = queue.take();
        }
    }

    public static void main(String[] args) throws Exception {
        try (final UniversalActiveMqQueue queue = new UniversalActiveMqQueue()) {
            queue.setUri("tcp://localhost:61616").setQueueName("ddth-queue").init();

            emptyQueue(queue);
            UniversalIdIntQueueMessage msg = queue.take();
            System.out.println("Queue empty: " + msg);

            msg = UniversalIdIntQueueMessage.newInstance();
            msg.content("Content: [" + System.currentTimeMillis() + "] " + new Date());
            System.out.println("Queue: " + queue.queue(msg));

            //            Thread.sleep(100);

            msg = queue.take();
            while (msg.qNumRequeues() < 2) {
                System.out.println("Message: " + msg);
                System.out.println("Content: " + new String(msg.content()));
                System.out.println("Requeue: " + queue.requeue(msg));

                //                Thread.sleep(100);

                msg = queue.take();
            }

            queue.finish(msg);
        }
    }
}

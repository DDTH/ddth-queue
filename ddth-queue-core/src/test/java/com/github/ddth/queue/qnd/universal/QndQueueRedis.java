package com.github.ddth.queue.qnd.universal;

import java.util.Date;

import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalRedisQueue;

public class QndQueueRedis {

    public static void main(String[] args) throws Exception {
        try (final UniversalRedisQueue queue = new UniversalRedisQueue()) {
            queue.setRedisHostAndPort("localhost:6379").init();

            UniversalIdIntQueueMessage msg = UniversalIdIntQueueMessage.newInstance();
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

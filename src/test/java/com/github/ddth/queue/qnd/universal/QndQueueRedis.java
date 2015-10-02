package com.github.ddth.queue.qnd.universal;

import java.util.Date;

import com.github.ddth.queue.impl.universal.UniversalQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalRedisQueue;

public class QndQueueRedis {

    public static void main(String[] args) throws Exception {
        final UniversalRedisQueue queue = new UniversalRedisQueue();
        queue.setRedisHostAndPort("localhost:6379").init();

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

        // Thread.sleep(3000);
        // Collection<IQueueMessage> orphanMsgs = queue
        // .getOrphanMessages(System.currentTimeMillis() - 1000L);
        // for (IQueueMessage orphanMsg : orphanMsgs) {
        // System.out.println("Orphan Msg: " + orphanMsg);
        // if (RandomUtils.nextInt(0, 100) < 70) {
        // System.out.println("...finish!");
        // queue.finish(orphanMsg);
        // } else {
        // System.out.print("...requeue...");
        // System.out.println(queue.moveFromEphemeralToQueueStorage(msg));
        // }
        // }

        queue.destroy();

    }
}

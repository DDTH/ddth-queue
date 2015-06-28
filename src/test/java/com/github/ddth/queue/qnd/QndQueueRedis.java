package com.github.ddth.queue.qnd;

import java.util.Collection;
import java.util.Date;

import org.apache.commons.lang3.RandomUtils;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.UniversalQueueMessage;
import com.github.ddth.queue.impl.UniversalRedisQueue;

public class QndQueueRedis {

    public static void main(String[] args) throws Exception {
        final UniversalRedisQueue queue = new UniversalRedisQueue();
        queue.setRedisHost("localhost").setRedisPort(6379).init();

        UniversalQueueMessage msg = new UniversalQueueMessage();
        String content = "Content: [" + System.currentTimeMillis() + "] " + new Date();
        msg.qNumRequeues(0).qOriginalTimestamp(new Date()).qTimestamp(new Date())
                .content(content.getBytes());
        System.out.println("Queue: " + queue.queue(msg));

        msg = queue.take();
        while (msg.qNumRequeues() < 2) {
            System.out.println(msg.content());
            System.out.println("Requeue: " + queue.requeue(msg));
            msg = queue.take();
        }

        // queue.finish(msg);

        Thread.sleep(3000);
        Collection<IQueueMessage> orphanMsgs = queue
                .getOrphanMessages(System.currentTimeMillis() - 1000L);
        for (IQueueMessage orphanMsg : orphanMsgs) {
            System.out.println("Orphan Msg: " + orphanMsg);
            if (RandomUtils.nextInt(0, 100) < 70) {
                System.out.println("...finish!");
                queue.finish(orphanMsg);
            } else {
                System.out.print("...requeue...");
                System.out.println(queue.moveFromEphemeralToQueueStorage(msg));
            }
        }

        queue.destroy();

    }
}

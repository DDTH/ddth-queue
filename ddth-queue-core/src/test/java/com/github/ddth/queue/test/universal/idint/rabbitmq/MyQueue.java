package com.github.ddth.queue.test.universal.idint.rabbitmq;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalRabbitMqQueue;

public class MyQueue extends UniversalRabbitMqQueue {
    public void flush() {
        int numMsgs = 0;
        long t1 = System.currentTimeMillis();
        IQueueMessage<Long, byte[]> msg = take();
        while (msg != null) {
            numMsgs++;
            msg = take();
        }
        msg = take();
        System.out.println("* Flush " + numMsgs + " msgs from queue in " + (System.currentTimeMillis() - t1) + "ms.");
    }
}

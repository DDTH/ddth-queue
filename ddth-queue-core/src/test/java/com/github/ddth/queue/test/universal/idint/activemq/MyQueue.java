package com.github.ddth.queue.test.universal.idint.activemq;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalActiveMqQueue;

public class MyQueue extends UniversalActiveMqQueue {
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

package com.github.ddth.queue.test.universal.idint.activemq;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalActiveMqQueue;
import com.github.ddth.queue.test.universal.BaseQueueLongTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.activemq.TestActiveMqQueueLong -DenableTestsActiveMq=true
 */

public class TestActiveMqQueueLong extends BaseQueueLongTest<Long> {
    public TestActiveMqQueueLong(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestActiveMqQueueLong.class);
    }

    private static class MyActiveMqQueue extends UniversalActiveMqQueue {
        public void flush() {
            int numMsgs = 0;
            long t1 = System.currentTimeMillis();
            IQueueMessage<Long, byte[]> msg = take();
            while (msg != null) {
                numMsgs++;
                msg = take();
            }
            msg = take();
            System.out.println("* Flush " + numMsgs + " msgs from queue in "
                    + (System.currentTimeMillis() - t1) + "ms.");
        }
    }

    protected IQueue<Long, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsActiveMq") == null) {
            return null;
        }
        String uri = System.getProperty("activemq.uri", "tcp://localhost:61616");
        String queueName = System.getProperty("activemq.queue", "ddth-queue");

        MyActiveMqQueue queue = new MyActiveMqQueue();
        queue.setUri(uri).setQueueName(queueName).init();
        queue.flush();
        return queue;
    }

    protected int numTestMessages() {
        // to make a very long queue
        return 16 * 1024;
    }

}

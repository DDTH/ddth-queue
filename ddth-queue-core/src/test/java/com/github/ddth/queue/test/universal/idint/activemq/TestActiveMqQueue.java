package com.github.ddth.queue.test.universal.idint.activemq;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalActiveMqQueue;
import com.github.ddth.queue.test.universal.BaseQueueFunctionalTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.activemq.TestActiveMqQueue -DenableTestsActiveMq=true
 */

public class TestActiveMqQueue extends BaseQueueFunctionalTest<Long> {
    public TestActiveMqQueue(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestActiveMqQueue.class);
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
            System.out.println(
                    "* Flush " + numMsgs + " msgs from queue in " + (System.currentTimeMillis()
                            - t1) + "ms.");
        }
    }

    protected IQueue<Long, byte[]> initQueueInstance(int ephemeralMaxSize) throws Exception {
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

}

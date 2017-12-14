package com.github.ddth.queue.test.universal.idstr.activemq;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.idstr.UniversalActiveMqQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;
import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.activemq.TestActiveMqQueueMT -DenableTestsActiveMq=true
 */

public class TestActiveMqQueueMT extends BaseQueueMultiThreadsTest<String> {
    public TestActiveMqQueueMT(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(
                TestActiveMqQueueMT.class);
    }

    private static class MyActiveMqQueue extends UniversalActiveMqQueue {
        public void flush() {
            int numMsgs = 0;
            long t1 = System.currentTimeMillis();
            IQueueMessage<String, byte[]> msg = take();
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

    protected IQueue<String, byte[]> initQueueInstance() throws Exception {
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
        return 8 * 1024;
    }

}

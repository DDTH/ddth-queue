package com.github.ddth.queue.test.universal;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;

import com.github.ddth.commons.utils.IdGenerator;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.AbstractQueue;
import com.github.ddth.queue.utils.QueueException;
import com.github.ddth.queue.utils.QueueException.EphemeralIsFull;
import com.github.ddth.queue.utils.QueueException.QueueIsFull;
import com.github.ddth.queue.utils.QueueUtils;

import junit.framework.TestCase;

public abstract class BaseQueueMultiThreadsTest<I> extends TestCase {

    protected static IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
    protected IQueue<I, byte[]> queue;
    protected Random random = new Random(System.currentTimeMillis());

    public BaseQueueMultiThreadsTest(String testName) {
        super(testName);
    }

    protected abstract IQueue<I, byte[]> initQueueInstance() throws Exception;

    protected void destroyQueueInstance(IQueue<?, ?> queue) {
        if (queue instanceof AbstractQueue) {
            ((AbstractQueue<?, ?>) queue).destroy();
        } else {
            throw new RuntimeException("[queue] is not closed!");
        }
    }

    protected static AtomicLong COUNTER_SENT;
    protected static AtomicLong COUNTER_RECEIVED;
    protected static List<String> STORAGE_SENT;
    protected static List<String> STORAGE_RECEIVED;

    @Before
    public void setUp() throws Exception {
        queue = initQueueInstance();
        COUNTER_SENT = new AtomicLong(0);
        COUNTER_RECEIVED = new AtomicLong(0);
        STORAGE_SENT = Collections.synchronizedList(new ArrayList<>());
        STORAGE_RECEIVED = Collections.synchronizedList(new ArrayList<>());
    }

    @After
    public void tearDown() {
        if (queue != null) {
            destroyQueueInstance(queue);
        }
    }

    protected void verify(int numMsgs) {
        assertEquals(numMsgs, COUNTER_SENT.get());
        assertEquals(numMsgs, COUNTER_RECEIVED.get());
        assertEquals(numMsgs, STORAGE_SENT.size());
        assertEquals(numMsgs, STORAGE_RECEIVED.size());

        Collections.sort(STORAGE_SENT);
        Collections.sort(STORAGE_RECEIVED);
        // if (STORAGE_SENT.equals(STORAGE_RECEIVED)) {
        // System.out.println(STORAGE_SENT);
        // System.out.println(STORAGE_RECEIVED);
        // }
        assertTrue(STORAGE_SENT.equals(STORAGE_RECEIVED));

        int queueSize = -1;
        try {
            queueSize = queue.queueSize();
        } catch (QueueException.OperationNotSupported e) {
            queueSize = -1;
        }
        assertTrue(queueSize == 0 || queueSize == -1);

        int ephemeralSize = -1;
        try {
            ephemeralSize = queue.ephemeralSize();
        } catch (QueueException.OperationNotSupported e) {
            ephemeralSize = -1;
        }
        assertTrue(ephemeralSize == 0 || ephemeralSize == -1);
    }

    protected Thread[] createProducerThreads(int numThreads, final int numMsgs) {
        Thread[] result = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            result[i] = new Thread("Producer - " + i) {
                public void run() {
                    for (int i = 0; i < numMsgs; i++) {
                        String content = idGen.generateId128Hex();
                        IQueueMessage<I, byte[]> msg = queue
                                .createMessage(content.getBytes(QueueUtils.UTF8));
                        try {
                            boolean status = false;
                            while (!status) {
                                try {
                                    status = queue.queue(msg);
                                    if (!status)
                                        Thread.sleep(1);
                                } catch (QueueIsFull | InterruptedException e) {
                                }
                            }
                            if (!STORAGE_SENT.add(content)) {
                                throw new IllegalStateException("Something wrong!");
                            }
                            COUNTER_SENT.incrementAndGet();
                        } catch (Exception e) {
                            e.printStackTrace();
                            break;
                        }
                    }
                }
            };
        }
        return result;
    }

    protected Thread[] createProducerThreadsDelay(int numThreads, final int delayMs,
            final int numMsgs) {
        Thread[] result = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            result[i] = new Thread("Producer - " + i) {
                public void run() {
                    for (int i = 0; i < numMsgs; i++) {
                        String content = idGen.generateId128Hex();
                        IQueueMessage<I, byte[]> msg = queue
                                .createMessage(content.getBytes(QueueUtils.UTF8));
                        try {
                            boolean status = false;
                            while (!status) {
                                try {
                                    status = queue.queue(msg);
                                    if (!status)
                                        Thread.sleep(1);
                                } catch (QueueIsFull | InterruptedException e) {
                                }
                            }
                            if (!STORAGE_SENT.add(content)) {
                                throw new IllegalStateException("Something wrong!");
                            }
                            COUNTER_SENT.incrementAndGet();
                            try {
                                Thread.sleep(random.nextInt(delayMs));
                            } catch (InterruptedException e) {
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            break;
                        }
                    }
                }
            };
        }
        return result;
    }

    protected Thread[] createConsumerThreads(int numThreads, final AtomicBoolean signal) {
        Thread[] result = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            result[i] = new Thread("Consumer - " + i) {
                public void run() {
                    while (!signal.get()) {
                        try {
                            IQueueMessage<I, byte[]> _msg = queue.take();
                            if (_msg != null) {
                                queue.finish(_msg);
                                String content = new String(_msg.qData(), QueueUtils.UTF8);
                                if (!STORAGE_RECEIVED.add(content)) {
                                    throw new IllegalStateException("Something wrong!");
                                }
                                COUNTER_RECEIVED.incrementAndGet();
                            } else {
                                try {
                                    Thread.sleep(1);
                                } catch (InterruptedException e) {
                                }
                            }
                        } catch (EphemeralIsFull e) {
                        } catch (Exception e) {
                            e.printStackTrace();
                            break;
                        }
                    }
                }
            };
        }
        return result;
    }

    protected Thread[] createConsumerThreadsDelay(int numThreads, final int delayMs,
            final AtomicBoolean signal) {
        Thread[] result = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            result[i] = new Thread("Consumer - " + i) {
                public void run() {
                    while (!signal.get()) {
                        try {
                            IQueueMessage<I, byte[]> _msg = queue.take();
                            if (_msg != null) {
                                queue.finish(_msg);
                                String content = new String(_msg.qData(), QueueUtils.UTF8);
                                if (!STORAGE_RECEIVED.add(content)) {
                                    throw new IllegalStateException("Something wrong!");
                                }
                                COUNTER_RECEIVED.incrementAndGet();
                                try {
                                    Thread.sleep(random.nextInt(delayMs));
                                } catch (InterruptedException e) {
                                }
                            } else {
                                try {
                                    Thread.sleep(1);
                                } catch (InterruptedException e) {
                                }
                            }
                        } catch (EphemeralIsFull e) {
                        } catch (Exception e) {
                            e.printStackTrace();
                            break;
                        }
                    }
                }
            };
        }
        return result;
    }
    /*----------------------------------------------------------------------*/

    protected int numTestMessages() {
        return 512 * 1024;
    }

    @org.junit.Test
    public void test1P1C() throws Exception {
        if (queue == null) {
            return;
        }

        final int NUM_MSGS = numTestMessages();
        final int NUM_PRODUCERS = 1;
        final int NUM_CONSUMER = 1;
        final AtomicBoolean SIGNAL = new AtomicBoolean(false);

        long t1 = System.currentTimeMillis();
        Thread[] producers = createProducerThreads(NUM_PRODUCERS, NUM_MSGS / NUM_PRODUCERS);
        Thread[] consumers = createConsumerThreads(NUM_CONSUMER, SIGNAL);
        for (Thread th : producers) {
            th.start();
        }
        for (Thread th : consumers) {
            th.start();
        }
        for (Thread th : producers) {
            th.join();
        }
        long t = System.currentTimeMillis();
        while (COUNTER_RECEIVED.get() < NUM_MSGS && t - t1 < 60000) {
            Thread.sleep(1);
            t = System.currentTimeMillis();
        }
        SIGNAL.set(true);
        for (Thread th : consumers) {
            th.join();
        }
        long d = t - t1;
        System.out.println(MessageFormat.format("== [{0}] TEST - {1}P{2}C",
                getClass().getSimpleName(), NUM_PRODUCERS, NUM_CONSUMER));
        System.out.println("  Msgs: " + NUM_MSGS + " - Sent: " + STORAGE_SENT.size()
                + " - Received: " + STORAGE_RECEIVED.size() + " / Duration: " + d + "ms - "
                + String.format("%,.1f", NUM_MSGS * 1000.0 / d) + " msg/s");
        verify(NUM_MSGS);
    }

    @org.junit.Test
    public void test1P4C() throws Exception {
        if (queue == null) {
            return;
        }

        final int NUM_MSGS = numTestMessages();
        final int NUM_PRODUCERS = 1;
        final int NUM_CONSUMER = 4;
        final AtomicBoolean SIGNAL = new AtomicBoolean(false);

        long t1 = System.currentTimeMillis();
        Thread[] producers = createProducerThreads(NUM_PRODUCERS, NUM_MSGS / NUM_PRODUCERS);
        Thread[] consumers = createConsumerThreads(NUM_CONSUMER, SIGNAL);
        for (Thread th : producers) {
            th.start();
        }
        for (Thread th : consumers) {
            th.start();
        }
        for (Thread th : producers) {
            th.join();
        }
        long t = System.currentTimeMillis();
        while (COUNTER_RECEIVED.get() < NUM_MSGS && t - t1 < 60000) {
            Thread.sleep(1);
            t = System.currentTimeMillis();
        }
        SIGNAL.set(true);
        for (Thread th : consumers) {
            th.join();
        }
        long d = t - t1;
        System.out.println(MessageFormat.format("== [{0}] TEST - {1}P{2}C",
                getClass().getSimpleName(), NUM_PRODUCERS, NUM_CONSUMER));
        System.out.println("  Msgs: " + NUM_MSGS + " - Sent: " + STORAGE_SENT.size()
                + " - Received: " + STORAGE_RECEIVED.size() + " / Duration: " + d + "ms - "
                + String.format("%,.1f", NUM_MSGS * 1000.0 / d) + " msg/s");
        verify(NUM_MSGS);
    }

    @org.junit.Test
    public void test4P1C() throws Exception {
        if (queue == null) {
            return;
        }

        final int NUM_MSGS = numTestMessages();
        final int NUM_PRODUCERS = 4;
        final int NUM_CONSUMER = 1;
        final AtomicBoolean SIGNAL = new AtomicBoolean(false);

        long t1 = System.currentTimeMillis();
        Thread[] producers = createProducerThreads(NUM_PRODUCERS, NUM_MSGS / NUM_PRODUCERS);
        Thread[] consumers = createConsumerThreads(NUM_CONSUMER, SIGNAL);
        for (Thread th : producers) {
            th.start();
        }
        for (Thread th : consumers) {
            th.start();
        }
        for (Thread th : producers) {
            th.join();
        }
        long t = System.currentTimeMillis();
        while (COUNTER_RECEIVED.get() < NUM_MSGS && t - t1 < 60000) {
            Thread.sleep(1);
            t = System.currentTimeMillis();
        }
        SIGNAL.set(true);
        for (Thread th : consumers) {
            th.join();
        }
        long d = t - t1;
        System.out.println(MessageFormat.format("== [{0}] TEST - {1}P{2}C",
                getClass().getSimpleName(), NUM_PRODUCERS, NUM_CONSUMER));
        System.out.println("  Msgs: " + NUM_MSGS + " - Sent: " + STORAGE_SENT.size()
                + " - Received: " + STORAGE_RECEIVED.size() + " / Duration: " + d + "ms - "
                + String.format("%,.1f", NUM_MSGS * 1000.0 / d) + " msg/s");
        verify(NUM_MSGS);
    }

    @org.junit.Test
    public void test4P4C() throws Exception {
        if (queue == null) {
            return;
        }

        final int NUM_MSGS = numTestMessages();
        final int NUM_PRODUCERS = 4;
        final int NUM_CONSUMER = 4;
        final AtomicBoolean SIGNAL = new AtomicBoolean(false);

        long t1 = System.currentTimeMillis();
        Thread[] producers = createProducerThreads(NUM_PRODUCERS, NUM_MSGS / NUM_PRODUCERS);
        Thread[] consumers = createConsumerThreads(NUM_CONSUMER, SIGNAL);
        for (Thread th : producers) {
            th.start();
        }
        for (Thread th : consumers) {
            th.start();
        }
        for (Thread th : producers) {
            th.join();
        }
        long t = System.currentTimeMillis();
        while (COUNTER_RECEIVED.get() < NUM_MSGS && t - t1 < 60000) {
            Thread.sleep(1);
            t = System.currentTimeMillis();
        }
        SIGNAL.set(true);
        for (Thread th : consumers) {
            th.join();
        }
        long d = t - t1;
        System.out.println(MessageFormat.format("== [{0}] TEST - {1}P{2}C",
                getClass().getSimpleName(), NUM_PRODUCERS, NUM_CONSUMER));
        System.out.println("  Msgs: " + NUM_MSGS + " - Sent: " + STORAGE_SENT.size()
                + " - Received: " + STORAGE_RECEIVED.size() + " / Duration: " + d + "ms - "
                + String.format("%,.1f", NUM_MSGS * 1000.0 / d) + " msg/s");
        verify(NUM_MSGS);
    }
}

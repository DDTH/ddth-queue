package com.github.ddth.queue.test.universal2;

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
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessage;
import com.github.ddth.queue.utils.QueueException;

import junit.framework.TestCase;

public abstract class BaseQueueLongTest extends TestCase {

    protected static IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
    protected IQueue queue;
    protected Random random = new Random(System.currentTimeMillis());

    public BaseQueueLongTest(String testName) {
        super(testName);
    }

    protected abstract IQueue initQueueInstance() throws Exception;

    protected abstract void destroyQueueInstance(IQueue queue);

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
                        String msgContent = idGen.generateId128Hex();
                        UniversalIdStrQueueMessage msg = UniversalIdStrQueueMessage.newInstance();
                        msg.content(msgContent);
                        try {
                            while (!queue.queue(msg)) {
                                try {
                                    Thread.sleep(1);
                                } catch (InterruptedException e) {
                                }
                            }
                            if (!STORAGE_SENT.add(msgContent)) {
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
                        String msgContent = idGen.generateId128Hex();
                        UniversalIdStrQueueMessage msg = UniversalIdStrQueueMessage.newInstance();
                        msg.content(msgContent);
                        try {
                            while (!queue.queue(msg)) {
                                try {
                                    Thread.sleep(1);
                                } catch (InterruptedException e) {
                                }
                            }
                            if (!STORAGE_SENT.add(msgContent)) {
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
                            IQueueMessage _msg = queue.take();
                            if (_msg instanceof UniversalIdStrQueueMessage) {
                                UniversalIdStrQueueMessage msg = (UniversalIdStrQueueMessage) _msg;
                                queue.finish(msg);
                                if (!STORAGE_RECEIVED.add(msg.contentAsString())) {
                                    throw new IllegalStateException("Something wrong!");
                                }
                                COUNTER_RECEIVED.incrementAndGet();
                            } else if (_msg == null) {
                                try {
                                    Thread.sleep(1);
                                } catch (InterruptedException e) {
                                }
                            } else {
                                throw new IllegalStateException("Something wrong!");
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

    protected Thread[] createConsumerThreadsDelay(int numThreads, final int delayMs,
            final AtomicBoolean signal) {
        Thread[] result = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            result[i] = new Thread("Consumer - " + i) {
                public void run() {
                    while (!signal.get()) {
                        try {
                            IQueueMessage _msg = queue.take();
                            if (_msg instanceof UniversalIdStrQueueMessage) {
                                UniversalIdStrQueueMessage msg = (UniversalIdStrQueueMessage) _msg;
                                queue.finish(msg);
                                if (!STORAGE_RECEIVED.add(msg.contentAsString())) {
                                    throw new IllegalStateException("Something wrong!");
                                }
                                COUNTER_RECEIVED.incrementAndGet();
                                try {
                                    Thread.sleep(random.nextInt(delayMs));
                                } catch (InterruptedException e) {
                                }
                            } else if (_msg == null) {
                                try {
                                    Thread.sleep(1);
                                } catch (InterruptedException e) {
                                }
                            } else {
                                throw new IllegalStateException("Something wrong!");
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
    /*----------------------------------------------------------------------*/

    protected int numTestMessages() {
        return 1024 * 1024;
    }

    private long TIMEOUT = 120000;

    private void makeLongQueue(int numProducers, int numMsgs) throws InterruptedException {
        Thread[] producers = createProducerThreads(numProducers, numMsgs / numProducers);
        for (Thread t : producers) {
            t.start();
        }
        for (Thread t : producers) {
            t.join();
        }
    }

    @org.junit.Test
    public void test1P1C() throws Exception {
        if (queue == null) {
            return;
        }

        int NUM_PRODUCERS = 1;
        int NUM_CONSUMER = 1;
        int NUM_MSGS = numTestMessages();

        long t1 = System.currentTimeMillis();
        makeLongQueue(NUM_PRODUCERS, NUM_MSGS);
        long t2 = System.currentTimeMillis();
        AtomicBoolean SIGNAL = new AtomicBoolean(false);
        Thread[] consumers = createConsumerThreads(NUM_CONSUMER, SIGNAL);
        for (Thread th : consumers) {
            th.start();
        }
        long t = System.currentTimeMillis();
        while (COUNTER_RECEIVED.get() < NUM_MSGS && t - t1 < TIMEOUT) {
            Thread.sleep(1);
            t = System.currentTimeMillis();
        }
        SIGNAL.set(true);
        for (Thread th : consumers) {
            th.join();
        }
        long d1 = t2 - t1;
        long d2 = t - t2;
        System.out.println(MessageFormat.format("== [{0}] TEST - {1}P{2}C",
                getClass().getSimpleName(), NUM_PRODUCERS, NUM_CONSUMER));
        System.out.println("  Msgs: " + NUM_MSGS + " - " + STORAGE_SENT.size() + " - "
                + STORAGE_RECEIVED.size() + " / Duration: " + d1 + "ms ("
                + String.format("%,.1f", NUM_MSGS * 1000.0 / d1) + " msg/s) - " + d2 + "ms ("
                + String.format("%,.1f", NUM_MSGS * 1000.0 / d2) + " msg/s)");
        verify(NUM_MSGS);
    }

    @org.junit.Test
    public void test1P4C() throws Exception {
        if (queue == null) {
            return;
        }

        int NUM_PRODUCERS = 1;
        int NUM_CONSUMER = 4;
        int NUM_MSGS = numTestMessages();

        long t1 = System.currentTimeMillis();
        makeLongQueue(NUM_PRODUCERS, NUM_MSGS);
        long t2 = System.currentTimeMillis();
        AtomicBoolean SIGNAL = new AtomicBoolean(false);
        Thread[] consumers = createConsumerThreads(NUM_CONSUMER, SIGNAL);
        for (Thread th : consumers) {
            th.start();
        }
        long t = System.currentTimeMillis();
        while (COUNTER_RECEIVED.get() < NUM_MSGS && t - t1 < TIMEOUT) {
            Thread.sleep(1);
            t = System.currentTimeMillis();
        }
        SIGNAL.set(true);
        for (Thread th : consumers) {
            th.join();
        }
        long d1 = t2 - t1;
        long d2 = t - t2;
        System.out.println(MessageFormat.format("== [{0}] TEST - {1}P{2}C",
                getClass().getSimpleName(), NUM_PRODUCERS, NUM_CONSUMER));
        System.out.println("  Msgs: " + NUM_MSGS + " - " + STORAGE_SENT.size() + " - "
                + STORAGE_RECEIVED.size() + " / Duration: " + d1 + "ms ("
                + String.format("%,.1f", NUM_MSGS * 1000.0 / d1) + " msg/s) - " + d2 + "ms ("
                + String.format("%,.1f", NUM_MSGS * 1000.0 / d2) + " msg/s)");
        verify(NUM_MSGS);
    }

    @org.junit.Test
    public void test4P1C() throws Exception {
        if (queue == null) {
            return;
        }

        int NUM_PRODUCERS = 4;
        int NUM_CONSUMER = 1;
        int NUM_MSGS = numTestMessages();

        long t1 = System.currentTimeMillis();
        makeLongQueue(NUM_PRODUCERS, NUM_MSGS);
        long t2 = System.currentTimeMillis();
        AtomicBoolean SIGNAL = new AtomicBoolean(false);
        Thread[] consumers = createConsumerThreads(NUM_CONSUMER, SIGNAL);
        for (Thread th : consumers) {
            th.start();
        }
        long t = System.currentTimeMillis();
        while (COUNTER_RECEIVED.get() < NUM_MSGS && t - t1 < TIMEOUT) {
            Thread.sleep(1);
            t = System.currentTimeMillis();
        }
        SIGNAL.set(true);
        for (Thread th : consumers) {
            th.join();
        }
        long d1 = t2 - t1;
        long d2 = t - t2;
        System.out.println(MessageFormat.format("== [{0}] TEST - {1}P{2}C",
                getClass().getSimpleName(), NUM_PRODUCERS, NUM_CONSUMER));
        System.out.println("  Msgs: " + NUM_MSGS + " - " + STORAGE_SENT.size() + " - "
                + STORAGE_RECEIVED.size() + " / Duration: " + d1 + "ms ("
                + String.format("%,.1f", NUM_MSGS * 1000.0 / d1) + " msg/s) - " + d2 + "ms ("
                + String.format("%,.1f", NUM_MSGS * 1000.0 / d2) + " msg/s)");
        verify(NUM_MSGS);
    }

    @org.junit.Test
    public void test4P4C() throws Exception {
        if (queue == null) {
            return;
        }

        int NUM_PRODUCERS = 4;
        int NUM_CONSUMER = 4;
        int NUM_MSGS = numTestMessages();

        long t1 = System.currentTimeMillis();
        makeLongQueue(NUM_PRODUCERS, NUM_MSGS);
        long t2 = System.currentTimeMillis();
        AtomicBoolean SIGNAL = new AtomicBoolean(false);
        Thread[] consumers = createConsumerThreads(NUM_CONSUMER, SIGNAL);
        for (Thread th : consumers) {
            th.start();
        }
        long t = System.currentTimeMillis();
        while (COUNTER_RECEIVED.get() < NUM_MSGS && t - t1 < TIMEOUT) {
            Thread.sleep(1);
            t = System.currentTimeMillis();
        }
        SIGNAL.set(true);
        for (Thread th : consumers) {
            th.join();
        }
        long d1 = t2 - t1;
        long d2 = t - t2;
        System.out.println(MessageFormat.format("== [{0}] TEST - {1}P{2}C",
                getClass().getSimpleName(), NUM_PRODUCERS, NUM_CONSUMER));
        System.out.println("  Msgs: " + NUM_MSGS + " - " + STORAGE_SENT.size() + " - "
                + STORAGE_RECEIVED.size() + " / Duration: " + d1 + "ms ("
                + String.format("%,.1f", NUM_MSGS * 1000.0 / d1) + " msg/s) - " + d2 + "ms ("
                + String.format("%,.1f", NUM_MSGS * 1000.0 / d2) + " msg/s)");
        verify(NUM_MSGS);
    }
}

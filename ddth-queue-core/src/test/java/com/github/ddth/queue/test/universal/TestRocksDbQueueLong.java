package com.github.ddth.queue.test.universal;

import java.io.File;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.io.FileUtils;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.universal.UniversalRocksDbQueue;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestRocksDbQueueLong extends BaseTest {
    public TestRocksDbQueueLong(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRocksDbQueueLong.class);
    }

    @Override
    protected IQueue initQueueInstance() {
        NUM_SENT = new AtomicLong(0);
        NUM_TAKEN = new AtomicLong(0);
        SIGNAL = new AtomicBoolean(false);
        SENT = new ConcurrentHashMap<Object, Object>();
        RECEIVE = new ConcurrentHashMap<Object, Object>();

        File tempDir = FileUtils.getTempDirectory();
        File testDir = new File(tempDir, String.valueOf(System.currentTimeMillis()));

        UniversalRocksDbQueue queue = new UniversalRocksDbQueue();
        queue.setStorageDir(testDir.getAbsolutePath()).init();
        return queue;
    }

    @Override
    protected void destroyQueueInstance(IQueue queue) {
        if (queue instanceof UniversalRocksDbQueue) {
            File dir = new File(((UniversalRocksDbQueue) queue).getStorageDir());
            ((UniversalRocksDbQueue) queue).destroy();
            FileUtils.deleteQuietly(dir);
        }
    }
    /*----------------------------------------------------------------------*/

    private static AtomicLong NUM_SENT = new AtomicLong(0);
    private static AtomicLong NUM_TAKEN = new AtomicLong(0);
    private static AtomicBoolean SIGNAL = new AtomicBoolean(false);
    private static ConcurrentMap<Object, Object> SENT = new ConcurrentHashMap<Object, Object>();
    private static ConcurrentMap<Object, Object> RECEIVE = new ConcurrentHashMap<Object, Object>();

    // to make a very long queue
    private final static int NUM_MSGS = 1024 * 1024;

    @org.junit.Test
    public void test1P1C() throws Exception {
        int NUM_PRODUCERS = 1;
        int NUM_CONSUMER = 1;

        long t1 = System.currentTimeMillis();
        Thread[] producers = createProducerThreads(NUM_PRODUCERS, NUM_MSGS / NUM_PRODUCERS,
                NUM_SENT, SENT);
        for (Thread t : producers) {
            t.start();
        }

        while (NUM_SENT.get() < NUM_MSGS) {
            Thread.sleep(1);
        }

        Thread[] consumers = createConsumerThreads(NUM_CONSUMER, SIGNAL, NUM_TAKEN, RECEIVE);
        for (Thread t : consumers) {
            t.start();
        }

        long t = System.currentTimeMillis();
        while (NUM_TAKEN.get() < NUM_MSGS && t - t1 < 60000) {
            Thread.sleep(1);
            t = System.currentTimeMillis();
        }
        SIGNAL.set(true);
        long d = t - t1;
        boolean checkResult = SENT.equals(RECEIVE);
        System.out.println("== [" + this.getClass().getSimpleName() + "] TEST - 1P / 1C");
        System.out.println("  Msgs: " + NUM_MSGS + " / " + NUM_SENT + " / " + NUM_TAKEN + " / "
                + checkResult + " / Rate: " + d + "ms / "
                + String.format("%,.1f", NUM_TAKEN.get() * 1000.0 / d) + " msg/s");
        assertTrue(checkResult);
    }

    @org.junit.Test
    public void test1P4C() throws Exception {
        int NUM_PRODUCERS = 1;
        int NUM_CONSUMER = 4;

        long t1 = System.currentTimeMillis();
        Thread[] producers = createProducerThreads(NUM_PRODUCERS, NUM_MSGS / NUM_PRODUCERS,
                NUM_SENT, SENT);
        for (Thread t : producers) {
            t.start();
        }

        while (NUM_SENT.get() < NUM_MSGS) {
            Thread.sleep(1);
        }

        Thread[] consumers = createConsumerThreads(NUM_CONSUMER, SIGNAL, NUM_TAKEN, RECEIVE);
        for (Thread t : consumers) {
            t.start();
        }

        long t = System.currentTimeMillis();
        while (NUM_TAKEN.get() < NUM_MSGS && t - t1 < 60000) {
            Thread.sleep(1);
            t = System.currentTimeMillis();
        }
        SIGNAL.set(true);
        long d = t - t1;
        boolean checkResult = SENT.equals(RECEIVE);
        System.out.println("== [" + this.getClass().getSimpleName() + "] TEST - 1P / 4C");
        System.out.println("  Msgs: " + NUM_MSGS + " / " + NUM_SENT + " / " + NUM_TAKEN + " / "
                + checkResult + " / Rate: " + d + "ms / "
                + String.format("%,.1f", NUM_TAKEN.get() * 1000.0 / d) + " msg/s");
        assertTrue(checkResult);
    }

    @org.junit.Test
    public void test4P1C() throws Exception {
        int NUM_PRODUCERS = 4;
        int NUM_CONSUMER = 1;

        long t1 = System.currentTimeMillis();
        Thread[] producers = createProducerThreads(NUM_PRODUCERS, NUM_MSGS / NUM_PRODUCERS,
                NUM_SENT, SENT);
        for (Thread t : producers) {
            t.start();
        }

        while (NUM_SENT.get() < NUM_MSGS) {
            Thread.sleep(1);
        }

        Thread[] consumers = createConsumerThreads(NUM_CONSUMER, SIGNAL, NUM_TAKEN, RECEIVE);
        for (Thread t : consumers) {
            t.start();
        }

        long t = System.currentTimeMillis();
        while (NUM_TAKEN.get() < NUM_MSGS && t - t1 < 60000) {
            Thread.sleep(1);
            t = System.currentTimeMillis();
        }
        SIGNAL.set(true);
        long d = t - t1;
        boolean checkResult = SENT.equals(RECEIVE);
        System.out.println("== [" + this.getClass().getSimpleName() + "] TEST - 4P / 1C");
        System.out.println("  Msgs: " + NUM_MSGS + " / " + NUM_SENT + " / " + NUM_TAKEN + " / "
                + checkResult + " / Rate: " + d + "ms / "
                + String.format("%,.1f", NUM_TAKEN.get() * 1000.0 / d) + " msg/s");
        assertTrue(checkResult);
    }

    @org.junit.Test
    public void test4P4C() throws Exception {
        int NUM_PRODUCERS = 4;
        int NUM_CONSUMER = 4;

        long t1 = System.currentTimeMillis();
        Thread[] producers = createProducerThreads(NUM_PRODUCERS, NUM_MSGS / NUM_PRODUCERS,
                NUM_SENT, SENT);
        for (Thread t : producers) {
            t.start();
        }

        while (NUM_SENT.get() < NUM_MSGS) {
            Thread.sleep(1);
        }

        Thread[] consumers = createConsumerThreads(NUM_CONSUMER, SIGNAL, NUM_TAKEN, RECEIVE);
        for (Thread t : consumers) {
            t.start();
        }

        long t = System.currentTimeMillis();
        while (NUM_TAKEN.get() < NUM_MSGS && t - t1 < 60000) {
            Thread.sleep(1);
            t = System.currentTimeMillis();
        }
        SIGNAL.set(true);
        long d = t - t1;
        boolean checkResult = SENT.equals(RECEIVE);
        System.out.println("== [" + this.getClass().getSimpleName() + "] TEST - 4P / 4C");
        System.out.println("  Msgs: " + NUM_MSGS + " / " + NUM_SENT + " / " + NUM_TAKEN + " / "
                + checkResult + " / Rate: " + d + "ms / "
                + String.format("%,.1f", NUM_TAKEN.get() * 1000.0 / d) + " msg/s");
        assertTrue(checkResult);
    }
}

package com.github.ddth.queue.qnd.disruptor;

import java.text.NumberFormat;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import com.lmax.disruptor.YieldingWaitStrategy;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;

public class QndDisruptor2 {

    static void qndSingleThread(int numItems) {
        final AtomicLong COUNTER_RECEIVED = new AtomicLong(0);
        final Disruptor<LongEvent> disruptor = new Disruptor<>(LongEvent.FACTORY, 128,
                Executors.defaultThreadFactory(), ProducerType.SINGLE, new YieldingWaitStrategy());
        disruptor.handleEventsWith(
                (event, sequence, endOfBatch) -> COUNTER_RECEIVED.incrementAndGet());
        disruptor.start();

        final long t = System.currentTimeMillis();
        for (int i = 0; i < numItems; i++) {
            disruptor.publishEvent((event, seq) -> event.set(seq));
        }
        long d = System.currentTimeMillis() - t;
        NumberFormat nf = NumberFormat.getInstance();
        System.out.println("========== qndSingleThread:");
        System.out.println("Sent: " + nf.format(numItems) + " / Received: "
                + nf.format(COUNTER_RECEIVED.get()) + " / Duration: " + d + " / Speed: "
                + NumberFormat.getInstance().format((numItems * 1000.0 / d)) + " items/sec");

        disruptor.shutdown();
    }

    static void qndMultiThreads(int numItems, int numThreads) throws InterruptedException {
        final AtomicLong COUNTER_RECEIVED = new AtomicLong(0);
        final Disruptor<LongEvent> disruptor = new Disruptor<>(LongEvent.FACTORY, 128,
                Executors.defaultThreadFactory(), ProducerType.MULTI, new YieldingWaitStrategy());
        disruptor.handleEventsWith(
                (event, sequence, endOfBatch) -> COUNTER_RECEIVED.incrementAndGet());
        disruptor.start();

        final long t = System.currentTimeMillis();
        final int numItemsPerThread = numItems / numThreads;
        final Thread[] THREADS = new Thread[numThreads];
        for (int i = 0; i < THREADS.length; i++) {
            THREADS[i] = new Thread() {
                public void run() {
                    for (int i = 0; i < numItemsPerThread; i++) {
                        disruptor.publishEvent((event, seq) -> event.set(seq));
                    }
                }
            };
            THREADS[i].start();
        }
        for (Thread thread : THREADS) {
            thread.join();
        }

        long d = System.currentTimeMillis() - t;
        NumberFormat nf = NumberFormat.getInstance();
        System.out.println("========== qndMultiThreads:");
        System.out.println("Sent: " + nf.format(numItems) + " / Received: "
                + nf.format(COUNTER_RECEIVED.get()) + " / Duration: " + d + " / Speed: "
                + NumberFormat.getInstance().format((numItems * 1000.0 / d)) + " items/sec");

        disruptor.shutdown();
    }

    public static void main(String[] args) throws Exception {
        qndSingleThread(100_000_000);
        qndMultiThreads(100_000_000, 4);
    }

}

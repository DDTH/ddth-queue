package com.github.ddth.queue.qnd.universal;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalDisruptorQueue;

public class QndMultithreadDisruptor {
    private static AtomicLong NUM_SENT = new AtomicLong(0);
    private static AtomicLong NUM_TAKEN = new AtomicLong(0);
    private static AtomicLong NUM_EXCEPTION = new AtomicLong(0);
    private static ConcurrentMap<Object, Object> SENT = new ConcurrentHashMap<Object, Object>();
    private static ConcurrentMap<Object, Object> RECEIVE = new ConcurrentHashMap<Object, Object>();
    private static AtomicLong TIMESTAMP = new AtomicLong(0);
    private static long NUM_ITEMS = 4 * 1024000;
    private static int NUM_THREADS = 8;

    public static void main(String[] args) throws Exception {
        try (final UniversalDisruptorQueue queue = new UniversalDisruptorQueue()) {
            queue.setRingSize(81920);
            queue.init();

            for (int i = 0; i < NUM_THREADS; i++) {
                Thread t = new Thread() {
                    public void run() {
                        while (true) {
                            try {
                                UniversalIdIntQueueMessage msg = queue.take();
                                if (msg != null) {
                                    // System.out.println(this + ": " + msg);
                                    queue.finish(msg);
                                    long numItems = NUM_TAKEN.incrementAndGet();
                                    if (numItems >= NUM_ITEMS) {
                                        TIMESTAMP.set(System.currentTimeMillis());
                                    }
                                    RECEIVE.put(new String(msg.getContent()), Boolean.TRUE);
                                } else {
                                    try {
                                        Thread.sleep(1);
                                    } catch (InterruptedException e) {
                                    }
                                }
                            } catch (Exception e) {
                                NUM_EXCEPTION.incrementAndGet();
                                e.printStackTrace();
                            }
                        }
                    }
                };
                t.setDaemon(true);
                t.start();
            }

            Thread.sleep(100);

            long t1 = System.currentTimeMillis();
            for (int i = 0; i < NUM_ITEMS; i++) {
                UniversalIdIntQueueMessage msg = UniversalIdIntQueueMessage.newInstance();
                String content = "Content: [" + i + "] " + new Date();
                msg.setContent(content);
                // System.out.println("Sending: " + msg.toJson());
                queue.queue(msg);
                NUM_SENT.incrementAndGet();
                SENT.put(new String(content), Boolean.TRUE);
                // Thread.sleep(1);
            }
            long t2 = System.currentTimeMillis();

            long t = System.currentTimeMillis();
            while (NUM_TAKEN.get() < NUM_ITEMS && t - t2 < 60000) {
                Thread.sleep(1);
                t = System.currentTimeMillis();
            }
            System.out.println("Duration Queue: " + (t2 - t1));
            System.out.println("Duration Take : " + (TIMESTAMP.get() - t1));
            System.out.println("Num sent     : " + NUM_SENT.get());
            System.out.println("Num taken    : " + NUM_TAKEN.get());
            System.out.println("Num exception: " + NUM_EXCEPTION.get());
            System.out.println("Sent size    : " + SENT.size());
            System.out.println("Receive size : " + RECEIVE.size());
            System.out.println("Check        : " + SENT.equals(RECEIVE));

            Thread.sleep(1000);
        }
    }
}

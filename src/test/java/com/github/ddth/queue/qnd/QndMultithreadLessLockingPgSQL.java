package com.github.ddth.queue.qnd;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.dbcp2.BasicDataSource;

import com.github.ddth.queue.UniversalQueueMessage;
import com.github.ddth.queue.impl.LessLockingUniversalPgSQLQueue;

public class QndMultithreadLessLockingPgSQL {

    private static AtomicLong NUM_SENT = new AtomicLong(0);
    private static AtomicLong NUM_TAKEN = new AtomicLong(0);
    private static AtomicLong NUM_EXCEPTION = new AtomicLong(0);
    private static ConcurrentMap<Object, Object> SENT = new ConcurrentHashMap<Object, Object>();
    private static ConcurrentMap<Object, Object> RECEIVE = new ConcurrentHashMap<Object, Object>();
    private static AtomicLong TIMESTAMP = new AtomicLong(0);
    private static long NUM_ITEMS = 8192;
    private static int NUM_THREADS = 8;

    public static void main(String[] args) throws Exception {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://localhost:5432/temp");
        dataSource.setUsername("test");
        dataSource.setPassword("test");

        final LessLockingUniversalPgSQLQueue queue = new LessLockingUniversalPgSQLQueue();
        queue.setFifo(false);
        queue.setTableName("queuell").setDataSource(dataSource).init();

        for (int i = 0; i < NUM_THREADS; i++) {
            Thread t = new Thread() {
                public void run() {
                    while (true) {
                        try {
                            UniversalQueueMessage msg = (UniversalQueueMessage) queue.take();
                            if (msg != null) {
                                // System.out.println(this + ": " + msg);
                                queue.finish(msg);
                                long numItems = NUM_TAKEN.incrementAndGet();
                                if (numItems == NUM_ITEMS) {
                                    TIMESTAMP.set(System.currentTimeMillis());
                                }
                                RECEIVE.put(new String(msg.content()), Boolean.TRUE);
                            } else {
                                try {
                                    Thread.sleep(10);
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

        Thread.sleep(2000);

        long t1 = System.currentTimeMillis();
        for (int i = 0; i < NUM_ITEMS; i++) {
            UniversalQueueMessage msg = new UniversalQueueMessage();
            String content = "Content: [" + i + "] " + new Date();
            msg.qNumRequeues(0).qOriginalTimestamp(new Date()).qTimestamp(new Date())
                    .content(content.getBytes());
            // System.out.println("Sending: " + msg.toJson());
            queue.queue(msg);
            NUM_SENT.incrementAndGet();
            SENT.put(new String(content), Boolean.TRUE);
            // Thread.sleep(1);
        }
        long t2 = System.currentTimeMillis();

        while (NUM_TAKEN.get() < NUM_ITEMS) {
            Thread.sleep(1);
        }
        System.out.println("Duration Queue: " + (t2 - t1));
        System.out.println("Duration Take : " + (TIMESTAMP.get() - t1));
        System.out.println("Num sent      : " + NUM_SENT.get());
        System.out.println("Num taken     : " + NUM_TAKEN.get());
        System.out.println("Num exception : " + NUM_EXCEPTION.get());
        System.out.println("Sent size     : " + SENT.size());
        System.out.println("Receive size  : " + RECEIVE.size());
        System.out.println("Check         : " + SENT.equals(RECEIVE));

        Thread.sleep(4000);
        System.exit(-1);
    }
}

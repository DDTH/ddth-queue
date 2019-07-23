package com.github.ddth.queue.qnd.universal;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.dbcp2.BasicDataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.idint.LessLockingUniversalMySQLQueue;

public class QndMultithreadLessLockingMySQL {
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
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl(
                "jdbc:mysql://localhost:3306/test?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8");
        dataSource.setUsername("test");
        dataSource.setPassword("test");

        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            jdbcHelper.setDataSource(dataSource).init();

            try (LessLockingUniversalMySQLQueue queue = new LessLockingUniversalMySQLQueue()) {
                queue.setTableName("queuell").setJdbcHelper(jdbcHelper).init();
                // queue.setMaxRetries(10);
                queue.setFifo(false);

                for (int i = 0; i < NUM_THREADS; i++) {
                    Thread t = new Thread() {
                        public void run() {
                            while (true) {
                                try {
                                    UniversalIdIntQueueMessage msg = (UniversalIdIntQueueMessage) queue
                                            .take();
                                    if (msg != null) {
                                        // System.out.println(this + ": " +
                                        // msg);
                                        queue.finish(msg);
                                        long numItems = NUM_TAKEN.incrementAndGet();
                                        if (numItems == NUM_ITEMS) {
                                            TIMESTAMP.set(System.currentTimeMillis());
                                        }
                                        RECEIVE.put(new String(msg.getContent()), Boolean.TRUE);
                                    } else {
                                        try {
                                            Thread.sleep(10);
                                        } catch (InterruptedException e) {
                                        }
                                    }
                                } catch (Exception e) {
                                    NUM_EXCEPTION.incrementAndGet();
                                    // e.printStackTrace();
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
                System.out.println("Num sent      : " + NUM_SENT.get());
                System.out.println("Num taken     : " + NUM_TAKEN.get());
                System.out.println("Num exception : " + NUM_EXCEPTION.get());
                System.out.println("Sent size     : " + SENT.size());
                System.out.println("Receive size  : " + RECEIVE.size());
                System.out.println("Check         : " + SENT.equals(RECEIVE));

                Thread.sleep(4000);
            }

            System.exit(-1);
        }
    }
}

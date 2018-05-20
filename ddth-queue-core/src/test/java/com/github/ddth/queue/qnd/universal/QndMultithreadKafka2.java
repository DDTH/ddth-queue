package com.github.ddth.queue.qnd.universal;

import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalKafkaQueue;
import com.github.ddth.queue.utils.QueueException;

public class QndMultithreadKafka2 {

    private static AtomicLong NUM_SENT = new AtomicLong(0);
    private static AtomicLong NUM_TAKEN = new AtomicLong(0);
    private static AtomicLong NUM_EXCEPTION = new AtomicLong(0);
    private static ConcurrentMap<Object, Object> SENT = new ConcurrentHashMap<Object, Object>();
    private static ConcurrentMap<Object, Object> RECEIVE = new ConcurrentHashMap<Object, Object>();
    private static AtomicLong TIMESTAMP = new AtomicLong(0);
    private static AtomicLong TOTAL_DELAY = new AtomicLong(0);
    private static long NUM_ITEMS = 102400;
    private static int NUM_THREADS = 8;
    private static String TOPIC = "t4partition";

    private static boolean DONE = false;

    private static void emptyQueue(IQueue<Long, byte[]> queue) throws Exception {
        long t1 = System.currentTimeMillis();
        System.out.println("Emptying queue...");
        long counter = 0;
        IQueueMessage<?, ?> msg;
        do {
            try {
                msg = queue.take();
                if (msg == null) {
                    Thread.sleep(2000);
                    msg = queue.take();
                }
            } catch (QueueException.CannotDeserializeQueueMessage e) {
                msg = new IQueueMessage.EmptyQueueMessage();
                e.printStackTrace();
            }
            if (msg != null) {
                counter++;
            }
        } while (msg != null);
        long t2 = System.currentTimeMillis();
        System.out.println("Emptying queue..." + counter + " in " + (t2 - t1) / 1000.0 + " secs");
    }

    private static void createThreads(final UniversalKafkaQueue queue, int numThreads) {
        final long DELAY_MS = 1;
        for (int i = 0; i < numThreads; i++) {
            Thread t = new Thread() {
                public void run() {
                    while (!DONE) {
                        try {
                            UniversalIdIntQueueMessage msg = queue.take();
                            if (msg != null) {
                                queue.finish(msg);
                                long numItems = NUM_TAKEN.incrementAndGet();
                                if (numItems >= numItems) {
                                    TIMESTAMP.set(System.currentTimeMillis());
                                }
                                RECEIVE.put(msg.contentAsString(), Boolean.TRUE);
                            } else {
                                TOTAL_DELAY.addAndGet(DELAY_MS);
                                try {
                                    Thread.sleep(DELAY_MS);
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
    }

    private static void queueMessages(IQueue<Long, byte[]> queue, long numItems) {
        for (int i = 0; i < NUM_ITEMS; i++) {
            UniversalIdIntQueueMessage msg = UniversalIdIntQueueMessage.newInstance();
            String content = "Content: [" + i + "] " + new Date();
            msg.content(content);
            queue.queue(msg);
            NUM_SENT.incrementAndGet();
            SENT.put(content, Boolean.TRUE);
        }
    }

    public static void main(String[] args) throws Exception {
        try (final UniversalKafkaQueue queue = new UniversalKafkaQueue()) {
            queue.setKafkaBootstrapServers("localhost:9092").setTopicName(TOPIC)
                    .setConsumerGroupId("ddth-queue").init();

            emptyQueue(queue);

            createThreads(queue, NUM_THREADS);

            long t1 = System.currentTimeMillis();
            queueMessages(queue, NUM_ITEMS);
            long t2 = System.currentTimeMillis();

            long t = System.currentTimeMillis();
            long lastNum = NUM_TAKEN.get();
            while ((NUM_TAKEN.get() < NUM_ITEMS && t - t2 < 180000) || lastNum < NUM_TAKEN.get()) {
                lastNum = NUM_TAKEN.get();
                Thread.sleep(1);
                t = System.currentTimeMillis();
            }
            DONE = true;
            System.out.println("Duration Queue: " + (t2 - t1));
            System.out.println("Duration Take : " + (TIMESTAMP.get() - t1));
            System.out.println("Total Delay   : " + TOTAL_DELAY.get());
            System.out.println("Num sent      : " + NUM_SENT.get());
            System.out.println("Num taken     : " + NUM_TAKEN.get());
            System.out.println("Num exception : " + NUM_EXCEPTION.get());
            System.out.println("Sent size     : " + SENT.size());
            System.out.println("Receive size  : " + RECEIVE.size());
            System.out.println("Check         : " + SENT.equals(RECEIVE));

            Thread.sleep(3000);
        }
    }
}

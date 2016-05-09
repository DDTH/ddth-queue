package com.github.ddth.queue.test.universal;

import java.util.Random;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;

import com.github.ddth.commons.utils.IdGenerator;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalQueueMessage;

import junit.framework.TestCase;

public abstract class BaseTest extends TestCase {

    protected static IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
    protected IQueue queue;
    protected Random random = new Random(System.currentTimeMillis());

    public BaseTest(String testName) {
        super(testName);
    }

    protected abstract IQueue initQueueInstance();

    protected abstract void destroyQueueInstance(IQueue queue);

    @Before
    public void setUp() {
        queue = initQueueInstance();
    }

    @After
    public void tearDown() {
        destroyQueueInstance(queue);
    }

    protected Thread[] createProducerThreads(int numThreads, final int numMsgs,
            final AtomicLong counterSent, final ConcurrentMap<Object, Object> sentStorage) {
        Thread[] result = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            result[i] = new Thread("Producer - " + i) {
                public void run() {
                    for (int i = 0; i < numMsgs; i++) {
                        String msgContent = idGen.generateId128Hex();
                        UniversalQueueMessage msg = UniversalQueueMessage.newInstance();
                        msg.content(msgContent);
                        try {
                            while (!queue.queue(msg)) {
                                try {
                                    Thread.sleep(1);
                                } catch (InterruptedException e) {
                                }
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            break;
                        }

                        counterSent.incrementAndGet();
                        sentStorage.put(msgContent, Boolean.TRUE);
                    }
                }
            };
        }
        return result;
    }

    protected Thread[] createProducerThreadsDelay(int numThreads, final int delayMs,
            final int numMsgs, final AtomicLong counterSent,
            final ConcurrentMap<Object, Object> sentStorage) {
        Thread[] result = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            result[i] = new Thread("Producer - " + i) {
                public void run() {
                    for (int i = 0; i < numMsgs; i++) {
                        String msgContent = idGen.generateId128Hex();
                        UniversalQueueMessage msg = UniversalQueueMessage.newInstance();
                        msg.content(msgContent);
                        try {
                            while (!queue.queue(msg)) {
                                try {
                                    Thread.sleep(1);
                                } catch (InterruptedException e) {
                                }
                            }
                            try {
                                Thread.sleep(random.nextInt(delayMs));
                            } catch (InterruptedException e) {
                            }
                        } catch (Exception e) {
                            e.printStackTrace();
                            break;
                        }

                        counterSent.incrementAndGet();
                        sentStorage.put(msgContent, Boolean.TRUE);
                    }
                }
            };
        }
        return result;
    }

    protected Thread[] createConsumerThreads(int numThreads, final AtomicBoolean signal,
            final AtomicLong counterReceived, final ConcurrentMap<Object, Object> receivedStorage) {
        Thread[] result = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            result[i] = new Thread("Consumer - " + i) {
                public void run() {
                    while (!signal.get()) {
                        try {
                            IQueueMessage _msg = queue.take();
                            if (_msg instanceof UniversalQueueMessage) {
                                UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
                                queue.finish(msg);
                                counterReceived.incrementAndGet();
                                receivedStorage.put(msg.contentAsString(), Boolean.TRUE);
                            } else {
                                try {
                                    Thread.sleep(1);
                                } catch (InterruptedException e) {
                                }
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
            final AtomicBoolean signal, final AtomicLong counterReceived,
            final ConcurrentMap<Object, Object> receivedStorage) {
        Thread[] result = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            result[i] = new Thread("Consumer - " + i) {
                public void run() {
                    while (!signal.get()) {
                        try {
                            IQueueMessage _msg = queue.take();
                            if (_msg instanceof UniversalQueueMessage) {
                                UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
                                queue.finish(msg);
                                counterReceived.incrementAndGet();
                                receivedStorage.put(msg.contentAsString(), Boolean.TRUE);
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
}

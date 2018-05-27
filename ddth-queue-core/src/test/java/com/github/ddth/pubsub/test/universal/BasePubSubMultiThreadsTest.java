package com.github.ddth.pubsub.test.universal;

import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.commons.lang3.builder.CompareToBuilder;
import org.junit.After;
import org.junit.Before;

import com.github.ddth.commons.utils.IdGenerator;
import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.ISubscriber;
import com.github.ddth.pubsub.impl.AbstractPubSubHub;
import com.github.ddth.queue.IMessage;
import com.google.common.util.concurrent.AtomicLongMap;

import junit.framework.TestCase;

public abstract class BasePubSubMultiThreadsTest<I> extends TestCase {

    protected static IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
    protected IPubSubHub<I, byte[]> hub;
    protected Random random = new Random(System.currentTimeMillis());

    public BasePubSubMultiThreadsTest(String testName) {
        super(testName);
    }

    protected abstract IPubSubHub<I, byte[]> initPubSubHubInstance() throws Exception;

    protected void destroyPubSubHubInstance(IPubSubHub<?, ?> hub) throws Exception {
        if (hub instanceof AbstractPubSubHub) {
            ((AbstractPubSubHub<?, ?>) hub).destroy();
        } else {
            throw new RuntimeException("[hub] is not closed!");
        }
        Thread.sleep(catchupSleepMs());
    }

    protected long catchupSleepMs() {
        return 0;
    }

    protected long randomSleepMs() {
        return 0;
    }

    private class MySubscriber implements ISubscriber<I, byte[]> {
        private List<IMessage<I, byte[]>> receivedMessages;
        private AtomicLong counter;
        private AtomicLongMap<String> counterMap;

        public MySubscriber(AtomicLong counter, AtomicLongMap<String> counterMap,
                List<IMessage<I, byte[]>> receivedMessages) {
            this.counter = counter;
            this.counterMap = counterMap;
            this.receivedMessages = receivedMessages;
        }

        @Override
        public boolean onMessage(String channel, IMessage<I, byte[]> msg) {
            receivedMessages.add(msg);
            counter.incrementAndGet();
            counterMap.incrementAndGet(channel);
            return true;
        }
    }

    @Before
    public void setUp() throws Exception {
        hub = initPubSubHubInstance();
    }

    @After
    public void tearDown() throws Exception {
        if (hub != null) {
            destroyPubSubHubInstance(hub);
        }
    }

    protected void verify(int numMsgs, AtomicLong counterReceived, AtomicLongMap<String> counterMap,
            List<IMessage<I, byte[]>> sentMessages, List<IMessage<I, byte[]>>[] receivedMessages) {
        Collections.sort(sentMessages, new Comparator<IMessage<I, byte[]>>() {
            @Override
            public int compare(IMessage<I, byte[]> o1, IMessage<I, byte[]> o2) {
                return CompareToBuilder.reflectionCompare(o1, o2, "data");
            }
        });

        assertEquals(numMsgs, counterReceived.get() / receivedMessages.length);
        assertEquals(numMsgs, sentMessages.size());
        for (List<IMessage<I, byte[]>> receivedList : receivedMessages) {
            Collections.sort(receivedList, new Comparator<IMessage<I, byte[]>>() {
                @Override
                public int compare(IMessage<I, byte[]> o1, IMessage<I, byte[]> o2) {
                    return CompareToBuilder.reflectionCompare(o1, o2, "data");
                }
            });

            assertEquals(numMsgs, receivedList.size());
            for (int i = 0; i < numMsgs; i++) {
                assertEquals(sentMessages.get(i), receivedList.get(i));
            }
        }
        counterMap.asMap().values()
                .forEach((v) -> assertEquals(numMsgs, v.intValue() / receivedMessages.length));
    }

    protected Thread[] createProducerThreads(String channel, int numThreads, int numMsgs,
            List<IMessage<I, byte[]>> sentMessages) {
        Thread[] result = new Thread[numThreads];
        for (int i = 0; i < numThreads; i++) {
            result[i] = new Thread("Producer - " + i) {
                public void run() {
                    for (int i = 0; i < numMsgs; i++) {
                        String content = idGen.generateId128Hex();
                        IMessage<I, byte[]> msg = hub.createMessage(content.getBytes());
                        try {
                            boolean status = false;
                            while (!status) {
                                status = hub.publish(channel, msg);
                                if (!status) {
                                    Thread.sleep(1);
                                }
                                if (randomSleepMs() != 0) {
                                    if (randomSleepMs() > 0) {
                                        Thread.sleep(random.nextInt((int) randomSleepMs() + 1));
                                    } else {
                                        Thread.sleep(random.nextInt(numThreads + 1));
                                    }
                                }
                            }
                            if (!sentMessages.add(msg)) {
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
        return 512 * 1024;
    }

    @SuppressWarnings("unchecked")
    private void doTest(int numProducers, int numConsumers) throws Exception {
        if (hub == null) {
            return;
        }

        AtomicLong counterReceived = new AtomicLong(0);
        AtomicLongMap<String> counterMap = AtomicLongMap.create();
        int numMgs = numTestMessages();
        List<IMessage<I, byte[]>>[] receivedMessages = new List[numConsumers];
        for (int i = 0; i < numConsumers; i++) {
            receivedMessages[i] = new ArrayList<>();
            hub.subscribe("demo",
                    new MySubscriber(counterReceived, counterMap, receivedMessages[i]));
        }
        Thread.sleep(catchupSleepMs());

        List<IMessage<I, byte[]>> sentMessages = Collections.synchronizedList(new ArrayList<>());

        long t1 = System.currentTimeMillis();
        Thread[] producers = createProducerThreads("demo", numProducers, numMgs / numProducers,
                sentMessages);
        for (Thread th : producers) {
            th.start();
        }
        for (Thread th : producers) {
            th.join();
        }
        long t = System.currentTimeMillis();
        while (counterReceived.get() < numConsumers && t - t1 < 60000) {
            Thread.sleep(1);
            t = System.currentTimeMillis();
        }
        long d = t - t1;
        Thread.sleep(catchupSleepMs());
        System.out.println(MessageFormat.format("== [{0}] TEST - {1}P{2}C",
                getClass().getSimpleName(), numProducers, numConsumers));
        System.out.println("  Num Msgs: " + numMgs + " - Published: " + sentMessages.size()
                + " - Received: " + counterReceived.get() / numConsumers + " / Duration: " + d
                + "ms - " + String.format("%,.1f", numMgs * 1000.0 / d) + " msg/s");
        verify(numMgs, counterReceived, counterMap, sentMessages, receivedMessages);
    }

    @org.junit.Test
    public void test1P1C() throws Exception {
        doTest(1, 1);
    }

    @org.junit.Test
    public void test4P1C() throws Exception {
        doTest(4, 1);
    }

    @org.junit.Test
    public void test1P4C() throws Exception {
        doTest(1, 4);
    }

    @org.junit.Test
    public void test4P4C() throws Exception {
        doTest(4, 4);
    }
}

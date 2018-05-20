package com.github.ddth.pubsub.test.universal;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import org.junit.After;
import org.junit.Before;

import com.github.ddth.commons.utils.IdGenerator;
import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.ISubscriber;
import com.github.ddth.pubsub.impl.AbstractPubSubHub;
import com.github.ddth.queue.IMessage;
import com.google.common.util.concurrent.AtomicLongMap;

import junit.framework.TestCase;

public abstract class BasePubSubFunctionalTest<I> extends TestCase {

    protected static IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
    protected IPubSubHub<I, byte[]> hub;

    public BasePubSubFunctionalTest(String testName) {
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

    private class MySubscriber implements ISubscriber<I, byte[]> {
        private Set<IMessage<I, byte[]>> receivedMessages;
        private AtomicLong counter;
        private AtomicLongMap<String> counterMap;

        public MySubscriber(AtomicLong counter, AtomicLongMap<String> counterMap,
                Set<IMessage<I, byte[]>> receivedMessages) {
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

    @org.junit.Test
    public void testPubSubOne() throws Exception {
        if (hub == null) {
            return;
        }
        AtomicLong counter = new AtomicLong(0);
        AtomicLongMap<String> counterMap = AtomicLongMap.create();
        Set<IMessage<I, byte[]>> receivedMessages = new HashSet<>();
        hub.subscribe("demo", new MySubscriber(counter, counterMap, receivedMessages));

        IMessage<I, byte[]> msg = hub.createMessage();
        hub.publish("demo", msg);
        Thread.sleep(catchupSleepMs());
        assertEquals(1, counter.get());
        assertEquals(1, counterMap.size());
        assertEquals(1, counterMap.get("demo"));
        assertEquals(0, counterMap.get("not-found"));
        assertEquals(1, receivedMessages.size());
        assertTrue(receivedMessages.contains(msg));
    }

    @org.junit.Test
    public void testPubSubSubscribeWrondChannel() throws Exception {
        if (hub == null) {
            return;
        }
        AtomicLong counter = new AtomicLong(0);
        AtomicLongMap<String> counterMap = AtomicLongMap.create();
        Set<IMessage<I, byte[]>> receivedMessages = new HashSet<>();
        hub.subscribe("demo1", new MySubscriber(counter, counterMap, receivedMessages));

        IMessage<I, byte[]> msg = hub.createMessage();
        hub.publish("demo", msg);
        Thread.sleep(catchupSleepMs());

        assertEquals(0, counter.get());
        assertEquals(0, counterMap.size());
        assertEquals(0, counterMap.get("demo"));
        assertEquals(0, counterMap.get("demo1"));
        assertEquals(0, counterMap.get("not-found"));
        assertEquals(0, receivedMessages.size());
        assertFalse(receivedMessages.contains(msg));
    }

    @org.junit.Test
    public void testPubSubNoSubscriber() throws Exception {
        if (hub == null) {
            return;
        }
        AtomicLong counter = new AtomicLong(0);
        AtomicLongMap<String> counterMap = AtomicLongMap.create();
        Set<IMessage<I, byte[]>> receivedMessages = new HashSet<>();

        IMessage<I, byte[]> msg = hub.createMessage();
        hub.publish("demo", msg);
        Thread.sleep(catchupSleepMs());

        assertEquals(0, counter.get());
        assertEquals(0, counterMap.size());
        assertEquals(0, counterMap.get("demo"));
        assertEquals(0, counterMap.get("not-found"));
        assertEquals(0, receivedMessages.size());
        assertFalse(receivedMessages.contains(msg));
    }

    @org.junit.Test
    public void testPubSubSubscribeUnsubscribe() throws Exception {
        if (hub == null) {
            return;
        }
        AtomicLong counter = new AtomicLong(0);
        AtomicLongMap<String> counterMap = AtomicLongMap.create();
        Set<IMessage<I, byte[]>> receivedMessages = new HashSet<>();

        ISubscriber<I, byte[]> subscriber = new MySubscriber(counter, counterMap, receivedMessages);
        hub.subscribe("demo", subscriber);

        IMessage<I, byte[]> msg1 = hub.createMessage();
        hub.publish("demo", msg1);
        Thread.sleep(catchupSleepMs());
        assertEquals(1, counter.get());
        assertEquals(1, counterMap.size());
        assertEquals(1, counterMap.get("demo"));
        assertEquals(0, counterMap.get("not-found"));
        assertEquals(1, receivedMessages.size());
        assertTrue(receivedMessages.contains(msg1));

        hub.unsubscribe("demo", subscriber);
        IMessage<I, byte[]> msg2 = hub.createMessage();
        hub.publish("demo", msg2);
        Thread.sleep(catchupSleepMs());
        assertEquals(1, counter.get());
        assertEquals(1, counterMap.size());
        assertEquals(1, counterMap.get("demo"));
        assertEquals(0, counterMap.get("not-found"));
        assertEquals(1, receivedMessages.size());
        assertFalse(receivedMessages.contains(msg2));
    }

    @org.junit.Test
    public void testPubSubMultiSubscribers() throws Exception {
        if (hub == null) {
            return;
        }
        AtomicLong counter = new AtomicLong(0);
        AtomicLongMap<String> counterMap = AtomicLongMap.create();
        Set<IMessage<I, byte[]>> receivedMessages1 = new HashSet<>();
        Set<IMessage<I, byte[]>> receivedMessages2 = new HashSet<>();
        Set<IMessage<I, byte[]>> receivedMessages3 = new HashSet<>();
        hub.subscribe("demo", new MySubscriber(counter, counterMap, receivedMessages1));
        hub.subscribe("demo", new MySubscriber(counter, counterMap, receivedMessages2));
        hub.subscribe("demo", new MySubscriber(counter, counterMap, receivedMessages3));

        IMessage<I, byte[]> msg1 = hub.createMessage();
        hub.publish("demo", msg1);
        Thread.sleep(catchupSleepMs());
        assertEquals(3, counter.get());
        assertEquals(1, counterMap.size());
        assertEquals(3, counterMap.get("demo"));
        assertEquals(0, counterMap.get("not-found"));
        assertEquals(1, receivedMessages1.size());
        assertTrue(receivedMessages1.contains(msg1));
        assertEquals(1, receivedMessages2.size());
        assertTrue(receivedMessages2.contains(msg1));
        assertEquals(1, receivedMessages3.size());
        assertTrue(receivedMessages3.contains(msg1));
    }

}

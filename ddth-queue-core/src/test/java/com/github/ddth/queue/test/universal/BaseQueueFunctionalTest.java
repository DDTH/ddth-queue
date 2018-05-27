package com.github.ddth.queue.test.universal;

import java.util.Collection;

import org.junit.After;
import org.junit.Before;

import com.github.ddth.commons.utils.IdGenerator;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.AbstractEphemeralSupportQueue;
import com.github.ddth.queue.impl.AbstractQueue;
import com.github.ddth.queue.utils.QueueException;
import com.github.ddth.queue.utils.QueueUtils;

import junit.framework.TestCase;

public abstract class BaseQueueFunctionalTest<I> extends TestCase {

    protected static IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
    protected IQueue<I, byte[]> queue;

    public BaseQueueFunctionalTest(String testName) {
        super(testName);
    }

    protected abstract IQueue<I, byte[]> initQueueInstance(int ephemeralMaxSize) throws Exception;

    protected void destroyQueueInstance(IQueue<?, ?> queue) {
        if (queue instanceof AbstractQueue) {
            ((AbstractQueue<?, ?>) queue).destroy();
        } else {
            throw new RuntimeException("[queue] is not closed!");
        }
    }

    protected final static int EPHEMERAL_MAX_SIZE = 2;

    @Before
    public void setUp() throws Exception {
        queue = initQueueInstance(EPHEMERAL_MAX_SIZE);
    }

    @After
    public void tearDown() {
        if (queue != null) {
            destroyQueueInstance(queue);
        }
    }

    /**
     * Empty queue:
     * 
     * <pre>
     * - Queue size = 0 (or not supported)
     * - Ephemeral size = 0 (or not supported)
     * - Orphan Message list must be empty
     * </pre>
     * 
     * @throws Exception
     */
    @org.junit.Test
    public void testEmptyQueue() throws Exception {
        if (queue == null) {
            return;
        }

        assertNull(queue.take());
        int queueSize = queue.queueSize();
        assertTrue(queueSize == 0 || queueSize == IQueue.SIZE_NOT_SUPPORTED);

        int ephemeralSize = queue.ephemeralSize();
        assertTrue(ephemeralSize == 0 || ephemeralSize == IQueue.SIZE_NOT_SUPPORTED);

        if (ephemeralSize >= 0) {
            Collection<IQueueMessage<I, byte[]>> orphanMessages = queue.getOrphanMessages(0);
            assertNotNull(orphanMessages);
            assertEquals(0, orphanMessages.size());
        }
    }

    /**
     * Queue one message:
     * 
     * <pre>
     * - Queue size = 1 (or not supported)
     * - Ephemeral size = 0 (or not supported)
     * - Orphan message list must be empty
     * </pre>
     * 
     * @throws Exception
     */
    @org.junit.Test
    public void testQueueOne() throws Exception {
        if (queue == null) {
            return;
        }

        String content = idGen.generateId128Ascii();
        IQueueMessage<I, byte[]> msg = queue.createMessage(content.getBytes(QueueUtils.UTF8));

        assertTrue(queue.queue(msg));
        int queueSize = queue.queueSize();
        assertTrue(queueSize == 1 || queueSize == IQueue.SIZE_NOT_SUPPORTED);
        int ephemeralSize = queue.ephemeralSize();
        assertTrue(ephemeralSize == 0 || ephemeralSize == IQueue.SIZE_NOT_SUPPORTED);

        if (ephemeralSize >= 0) {
            Collection<IQueueMessage<I, byte[]>> orphanMessages = queue.getOrphanMessages(0);
            assertNotNull(orphanMessages);
            assertEquals(0, orphanMessages.size());
        }
    }

    /**
     * 1. Queue one message:
     * 
     * <pre>
     * - Queue size = 1 (or not supported)
     * - Ephemeral size = 0 (or not supported)
     * - Orphan message list must be empty
     * </pre>
     * 
     * 2. Take one message:
     * 
     * <pre>
     * - Queue size = 0 (or not supported)
     * - Ephemeral size = 1 (or not supported)
     * - Orphan message list (long period) must be empty
     * - Orphan message list (short period) must contain 1 item
     * </pre>
     * 
     * @throws Exception
     */
    @org.junit.Test
    public void testQueueAndTake() throws Exception {
        if (queue == null) {
            return;
        }

        int queueSize, ephemeralSize;

        String content = idGen.generateId128Ascii();
        IQueueMessage<I, byte[]> msg1 = queue.createMessage(content.getBytes(QueueUtils.UTF8));

        assertTrue(queue.queue(msg1));
        queueSize = queue.queueSize();
        assertTrue(queueSize == 1 || queueSize == IQueue.SIZE_NOT_SUPPORTED);
        ephemeralSize = queue.ephemeralSize();
        assertTrue(ephemeralSize == 0 || ephemeralSize == IQueue.SIZE_NOT_SUPPORTED);
        if (ephemeralSize >= 0) {
            Collection<IQueueMessage<I, byte[]>> orphanMessages = queue.getOrphanMessages(0);
            assertNotNull(orphanMessages);
            assertEquals(0, orphanMessages.size());
        }

        long t = System.currentTimeMillis();
        IQueueMessage<I, byte[]> msg2 = queue.take();
        assertNotNull(msg2);
        assertEquals(msg1.getId(), msg2.getId());
        assertEquals(content, new String((byte[]) msg2.getData(), QueueUtils.UTF8));
        queueSize = queue.queueSize();
        assertTrue(queueSize == 0 || queueSize == IQueue.SIZE_NOT_SUPPORTED);
        ephemeralSize = queue.ephemeralSize();
        assertTrue(ephemeralSize == 1 || ephemeralSize == IQueue.SIZE_NOT_SUPPORTED);
        if (ephemeralSize >= 0) {
            Collection<IQueueMessage<I, byte[]>> orphanMessages = queue
                    .getOrphanMessages(System.currentTimeMillis() - t + 10000);
            assertNotNull(orphanMessages);
            assertEquals(0, orphanMessages.size());

            Thread.sleep(2000);
            orphanMessages = queue.getOrphanMessages(1000);
            assertNotNull(orphanMessages);
            assertEquals(1, orphanMessages.size());
            assertEquals(msg1.getId(), orphanMessages.iterator().next().getId());
            assertEquals(msg2.getId(), orphanMessages.iterator().next().getId());
        }
    }

    /**
     * 1. Queue one message:
     * 
     * <pre>
     * - Queue size = 1 (or not supported)
     * - Ephemeral size = 0 (or not supported)
     * - Orphan message list must be empty
     * </pre>
     * 
     * 2. Take one message:
     * 
     * <pre>
     * - Queue size = 0 (or not supported)
     * - Ephemeral size = 1 (or not supported)
     * - Orphan message list must be empty
     * </pre>
     * 
     * 3. Finish message:
     * 
     * <pre>
     * - Queue size = 0 (or not supported)
     * - Ephemeral size = 0 (or not supported)
     * - Orphan message list must be empty
     * </pre>
     * 
     * @throws Exception
     */
    @org.junit.Test
    public void testQueueTakeTakeAndFinish() throws Exception {
        if (queue == null) {
            return;
        }

        String content = idGen.generateId128Ascii();
        IQueueMessage<I, byte[]> msg1 = queue.createMessage(content.getBytes(QueueUtils.UTF8));

        int queueSize, ephemeralSize;

        assertTrue(queue.queue(msg1));
        queueSize = queue.queueSize();
        assertTrue(queueSize == 1 || queueSize == IQueue.SIZE_NOT_SUPPORTED);
        ephemeralSize = queue.ephemeralSize();
        assertTrue(ephemeralSize == 0 || ephemeralSize == IQueue.SIZE_NOT_SUPPORTED);
        if (ephemeralSize >= 0) {
            Collection<IQueueMessage<I, byte[]>> orphanMessages = queue.getOrphanMessages(0);
            assertNotNull(orphanMessages);
            assertEquals(0, orphanMessages.size());
        }

        IQueueMessage<I, byte[]> msg2 = queue.take();
        assertNotNull(msg2);
        assertEquals(msg1.getId(), msg2.getId());
        assertEquals(content, new String((byte[]) msg2.getData(), QueueUtils.UTF8));
        queueSize = queue.queueSize();
        assertTrue(queueSize == 0 || queueSize == IQueue.SIZE_NOT_SUPPORTED);
        ephemeralSize = queue.ephemeralSize();
        assertTrue(ephemeralSize == 1 || ephemeralSize == IQueue.SIZE_NOT_SUPPORTED);
        if (ephemeralSize >= 0) {
            Thread.sleep(2000);
            Collection<IQueueMessage<I, byte[]>> orphanMessages = queue.getOrphanMessages(1000);
            assertNotNull(orphanMessages);
            assertEquals(1, orphanMessages.size());
            assertEquals(msg1.getId(), orphanMessages.iterator().next().getId());
            assertEquals(msg2.getId(), orphanMessages.iterator().next().getId());
        }

        queue.finish(msg2);
        assertNull(queue.take());
        queueSize = queue.queueSize();
        assertTrue(queueSize == 0 || queueSize == IQueue.SIZE_NOT_SUPPORTED);
        ephemeralSize = queue.ephemeralSize();
        assertTrue(ephemeralSize == 0 || ephemeralSize == IQueue.SIZE_NOT_SUPPORTED);
        if (ephemeralSize >= 0) {
            Collection<IQueueMessage<I, byte[]>> orphanMessages = queue.getOrphanMessages(0);
            assertNotNull(orphanMessages);
            assertEquals(0, orphanMessages.size());
        }
    }

    /**
     * 1. Queue one message:
     * 
     * <pre>
     * - Queue size = 1 (or not supported)
     * - Ephemeral size = 0 (or not supported)
     * </pre>
     * 
     * 2. Take one message:
     * 
     * <pre>
     * - Queue size = 0 (or not supported)
     * - Ephemeral size = 0 (or not supported)
     * </pre>
     * 
     * 3. Finish message:
     * 
     * <pre>
     * - Queue size = 0 (or not supported)
     * - Ephemeral size = 0 (or not supported)
     * </pre>
     * 
     * @throws Exception
     */
    @org.junit.Test
    public void testEphemeralDisabled() throws Exception {
        if (queue == null) {
            return;
        }
        if (!(queue instanceof AbstractEphemeralSupportQueue)) {
            return;
        }
        ((AbstractEphemeralSupportQueue<I, byte[]>) queue).setEphemeralDisabled(true);

        int queueSize, ephemeralSize;

        String content = idGen.generateId128Ascii();
        IQueueMessage<I, byte[]> msg1 = queue.createMessage(content.getBytes(QueueUtils.UTF8));

        assertTrue(queue.queue(msg1));
        queueSize = queue.queueSize();
        assertTrue(queueSize == 1 || queueSize == IQueue.SIZE_NOT_SUPPORTED);
        ephemeralSize = queue.ephemeralSize();
        assertTrue(ephemeralSize == 0 || ephemeralSize == IQueue.SIZE_NOT_SUPPORTED);

        IQueueMessage<I, byte[]> msg2 = queue.take();
        assertNotNull(msg2);
        assertEquals(msg1.getId(), msg2.getId());
        assertEquals(content, new String((byte[]) msg2.getData(), QueueUtils.UTF8));
        queueSize = queue.queueSize();
        assertTrue(queueSize == 0 || queueSize == IQueue.SIZE_NOT_SUPPORTED);
        ephemeralSize = queue.ephemeralSize();
        assertTrue(ephemeralSize == 0 || ephemeralSize == IQueue.SIZE_NOT_SUPPORTED);

        queue.finish(msg2);
        assertNull(queue.take());
        queueSize = queue.queueSize();
        assertTrue(queueSize == 0 || queueSize == IQueue.SIZE_NOT_SUPPORTED);
        ephemeralSize = queue.ephemeralSize();
        assertTrue(ephemeralSize == 0 || ephemeralSize == IQueue.SIZE_NOT_SUPPORTED);
    }

    /**
     * 1. Queue <ephemeral-max-size>+1 messages
     * 
     * <pre>
     * - Queue size = i (or not supported)
     * - Ephemeral size = 0 (or not supported)
     * - Orphan message list must be empty
     * </pre>
     * 
     * 2. Take <ephemeral-max-size> messages
     * 
     * <pre>
     * - Queue size = n-i (or not supported)
     * - Ephemeral size = i (or not supported)
     * </pre>
     * 
     * 3. Take one more message
     * 
     * <pre>
     * - Exception {@link QueueException.EphemeralIsFull} must be thrown.
     * </pre>
     * 
     * 4. Finish taken messages:
     * 
     * <pre>
     * - Queue size = 1 (or not supported)
     * - Ephemeral size = n-i (or not supported)
     * </pre>
     * 
     * 5. Take one message:
     * 
     * <pre>
     * - Queue size = 0 (or not supported)
     * - Ephemeral size = 1 (or not supported)
     * </pre>
     * 
     * 6. Finish message:
     * 
     * <pre>
     * - Queue size = 0 (or not supported)
     * - Ephemeral size = 0 (or not supported)
     * </pre>
     * 
     * @throws Exception
     */
    @org.junit.Test
    public void testEphemeralMaxSize() throws Exception {
        if (queue == null) {
            return;
        }
        if (!(queue instanceof AbstractEphemeralSupportQueue)) {
            return;
        }
        ((AbstractEphemeralSupportQueue<I, byte[]>) queue).setEphemeralDisabled(false)
                .setEphemeralMaxSize(EPHEMERAL_MAX_SIZE);
        int queueSize, ephemeralSize;

        for (int i = 0, n = EPHEMERAL_MAX_SIZE + 1; i < n; i++) {
            String content = idGen.generateId128Ascii();
            IQueueMessage<I, byte[]> msg = queue.createMessage(content.getBytes(QueueUtils.UTF8));
            assertTrue(queue.queue(msg));
            queueSize = queue.queueSize();
            assertTrue(queueSize == i + 1 || queueSize == IQueue.SIZE_NOT_SUPPORTED);
            ephemeralSize = queue.ephemeralSize();
            assertTrue(ephemeralSize == 0 || ephemeralSize == IQueue.SIZE_NOT_SUPPORTED);
            if (ephemeralSize >= 0) {
                Collection<IQueueMessage<I, byte[]>> orphanMessages = queue.getOrphanMessages(0);
                assertNotNull(orphanMessages);
                assertEquals(0, orphanMessages.size());
            }
        }

        @SuppressWarnings("unchecked")
        IQueueMessage<I, byte[]>[] MSGS = new IQueueMessage[EPHEMERAL_MAX_SIZE];
        for (int i = 0; i < EPHEMERAL_MAX_SIZE; i++) {
            MSGS[i] = queue.take();
            assertNotNull(MSGS[i]);
            assertEquals(EPHEMERAL_MAX_SIZE - i, queue.queueSize());
            assertEquals(i + 1, queue.ephemeralSize());
        }

        boolean ephemeralIsFull = false;
        try {
            IQueueMessage<I, byte[]> msg = queue.take();
            assertNull(msg);
        } catch (QueueException.EphemeralIsFull e) {
            ephemeralIsFull = true;
        }
        assertTrue(ephemeralIsFull);

        for (int i = 0; i < EPHEMERAL_MAX_SIZE; i++) {
            queue.finish(MSGS[i]);
            assertEquals(1, queue.queueSize());
            assertEquals(EPHEMERAL_MAX_SIZE - i - 1, queue.ephemeralSize());
        }

        IQueueMessage<I, byte[]> msg = queue.take();
        assertNotNull(msg);
        queueSize = queue.queueSize();
        assertTrue(queueSize == 0 || queueSize == IQueue.SIZE_NOT_SUPPORTED);
        ephemeralSize = queue.ephemeralSize();
        assertTrue(ephemeralSize == 1 || ephemeralSize == IQueue.SIZE_NOT_SUPPORTED);

        queue.finish(msg);
        queueSize = queue.queueSize();
        assertTrue(queueSize == 0 || queueSize == IQueue.SIZE_NOT_SUPPORTED);
        ephemeralSize = queue.ephemeralSize();
        assertTrue(ephemeralSize == 0 || ephemeralSize == IQueue.SIZE_NOT_SUPPORTED);
    }
}

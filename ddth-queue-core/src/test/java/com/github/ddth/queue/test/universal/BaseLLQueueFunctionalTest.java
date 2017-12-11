//package com.github.ddth.queue.test.universal;
//
//import java.util.Collection;
//
//import org.junit.After;
//import org.junit.Before;
//
//import com.github.ddth.commons.utils.IdGenerator;
//import com.github.ddth.queue.IQueue;
//import com.github.ddth.queue.IQueueMessage;
//import com.github.ddth.queue.impl.AbstractEphemeralSupportQueue;
//import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
//import com.github.ddth.queue.utils.QueueException;
//
//import junit.framework.TestCase;
//
//public abstract class BaseLLQueueFunctionalTest extends TestCase {
//
//    protected static IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
//    protected IQueue queue;
//
//    public BaseLLQueueFunctionalTest(String testName) {
//        super(testName);
//    }
//
//    protected abstract IQueue initQueueInstance(int ephemeralMaxSize) throws Exception;
//
//    protected abstract void destroyQueueInstance(IQueue queue);
//
//    protected final static int EPHEMERAL_MAX_SIZE = 2;
//
//    @Before
//    public void setUp() throws Exception {
//        queue = initQueueInstance(EPHEMERAL_MAX_SIZE);
//    }
//
//    @After
//    public void tearDown() {
//        if (queue != null) {
//            destroyQueueInstance(queue);
//        }
//    }
//
//    @org.junit.Test
//    public void testEmptyQueue() throws Exception {
//        if (queue == null) {
//            return;
//        }
//
//        assertNull(queue.take());
//        int queueSize = queue.queueSize();
//        assertTrue(queueSize == 0 || queueSize < 0);
//
//        int ephemeralSize = queue.ephemeralSize();
//        assertTrue(ephemeralSize == 0 || ephemeralSize < 0);
//
//        if (ephemeralSize >= 0) {
//            Collection<IQueueMessage> orphanMessages = queue.getOrphanMessages(1);
//            assertNotNull(orphanMessages);
//            assertEquals(0, orphanMessages.size());
//        }
//    }
//
//    @org.junit.Test
//    public void testQueueOne() throws Exception {
//        if (queue == null) {
//            return;
//        }
//
//        String content = idGen.generateId128Ascii();
//        UniversalIdIntQueueMessage msg = UniversalIdIntQueueMessage.newInstance();
//        msg.content(content);
//
//        assertTrue(queue.queue(msg));
//        int queueSize = queue.queueSize();
//        assertTrue(queueSize == 1 || queueSize < 0);
//        int ephemeralSize = queue.ephemeralSize();
//        assertTrue(ephemeralSize == 0 || ephemeralSize < 0);
//
//        if (ephemeralSize >= 0) {
//            Collection<IQueueMessage> orphanMessages = queue.getOrphanMessages(1);
//            assertNotNull(orphanMessages);
//            assertEquals(0, orphanMessages.size());
//        }
//    }
//
//    @org.junit.Test
//    public void testQueueAndTake() throws Exception {
//        if (queue == null) {
//            return;
//        }
//
//        int queueSize, ephemeralSize;
//
//        String content = idGen.generateId128Ascii();
//        UniversalIdIntQueueMessage msg1 = UniversalIdIntQueueMessage.newInstance();
//        msg1.content(content);
//
//        assertTrue(queue.queue(msg1));
//        queueSize = queue.queueSize();
//        assertTrue(queueSize == 1 || queueSize < 0);
//        ephemeralSize = queue.ephemeralSize();
//        assertTrue(ephemeralSize == 0 || ephemeralSize < 0);
//
//        UniversalIdIntQueueMessage msg2 = (UniversalIdIntQueueMessage) queue.take();
//        assertNotNull(msg2);
//        assertEquals(content, msg2.contentAsString());
//        queueSize = queue.queueSize();
//        assertTrue(queueSize == 0 || queueSize < 0);
//        ephemeralSize = queue.ephemeralSize();
//        assertTrue(ephemeralSize == 1 || ephemeralSize < 0);
//
//        if (ephemeralSize >= 0) {
//            Collection<IQueueMessage> orphanMessages = queue.getOrphanMessages(10000);
//            assertNotNull(orphanMessages);
//            assertEquals(0, orphanMessages.size());
//
//            Thread.sleep(2000);
//            orphanMessages = queue.getOrphanMessages(1000);
//            assertNotNull(orphanMessages);
//            assertEquals(1, orphanMessages.size());
//        }
//    }
//
//    @org.junit.Test
//    public void testQueueTakeTakeAndFinish() throws Exception {
//        if (queue == null) {
//            return;
//        }
//
//        String content = idGen.generateId128Ascii();
//        UniversalIdIntQueueMessage msg1 = UniversalIdIntQueueMessage.newInstance();
//        msg1.content(content);
//
//        int queueSize, ephemeralSize;
//
//        assertTrue(queue.queue(msg1));
//        queueSize = queue.queueSize();
//        assertTrue(queueSize == 1 || queueSize < 0);
//        ephemeralSize = queue.ephemeralSize();
//        assertTrue(ephemeralSize == 0 || ephemeralSize < 0);
//
//        if (ephemeralSize >= 0) {
//            Collection<IQueueMessage> orphanMessages = queue.getOrphanMessages(1);
//            assertNotNull(orphanMessages);
//            assertEquals(0, orphanMessages.size());
//        }
//
//        UniversalIdIntQueueMessage msg2 = (UniversalIdIntQueueMessage) queue.take();
//        assertNotNull(msg2);
//        assertEquals(content, msg2.contentAsString());
//        queueSize = queue.queueSize();
//        assertTrue(queueSize == 0 || queueSize < 0);
//        ephemeralSize = queue.ephemeralSize();
//        assertTrue(ephemeralSize == 1 || ephemeralSize < 0);
//
//        if (ephemeralSize >= 0) {
//            Thread.sleep(2000);
//            Collection<IQueueMessage> orphanMessages = queue.getOrphanMessages(1000);
//            assertNotNull(orphanMessages);
//            assertEquals(1, orphanMessages.size());
//        }
//
//        queue.finish(msg2);
//        assertNull(queue.take());
//        queueSize = queue.queueSize();
//        assertTrue(queueSize == 0 || queueSize < 0);
//        ephemeralSize = queue.ephemeralSize();
//        assertTrue(ephemeralSize == 0 || ephemeralSize < 0);
//
//        if (ephemeralSize >= 0) {
//            Collection<IQueueMessage> orphanMessages = queue.getOrphanMessages(1);
//            assertNotNull(orphanMessages);
//            assertEquals(0, orphanMessages.size());
//        }
//    }
//
//    @org.junit.Test
//    public void testEphemeralDisabled() throws Exception {
//        if (queue == null) {
//            return;
//        }
//        if (!(queue instanceof AbstractEphemeralSupportQueue)) {
//            return;
//        }
//        ((AbstractEphemeralSupportQueue) queue).setEphemeralDisabled(true);
//
//        String content = idGen.generateId128Ascii();
//        UniversalIdIntQueueMessage msg1 = UniversalIdIntQueueMessage.newInstance();
//        msg1.content(content);
//
//        assertTrue(queue.queue(msg1));
//        assertEquals(1, queue.queueSize());
//        assertEquals(0, queue.ephemeralSize());
//
//        UniversalIdIntQueueMessage msg2 = (UniversalIdIntQueueMessage) queue.take();
//        assertNotNull(msg2);
//        assertEquals(content, msg2.contentAsString());
//        assertEquals(0, queue.queueSize());
//        assertEquals(0, queue.ephemeralSize());
//
//        queue.finish(msg2);
//        assertNull(queue.take());
//        assertEquals(0, queue.queueSize());
//        assertEquals(0, queue.ephemeralSize());
//    }
//
//    @org.junit.Test
//    public void testEphemeralMaxSize() throws Exception {
//        if (queue == null) {
//            return;
//        }
//        if (!(queue instanceof AbstractEphemeralSupportQueue)) {
//            return;
//        }
//
//        for (int i = 0, n = EPHEMERAL_MAX_SIZE + 1; i < n; i++) {
//            String content = idGen.generateId128Ascii();
//            UniversalIdIntQueueMessage msg = UniversalIdIntQueueMessage.newInstance();
//            msg.content(content);
//            assertTrue(queue.queue(msg));
//            assertEquals(i + 1, queue.queueSize());
//            assertEquals(0, queue.ephemeralSize());
//        }
//
//        IQueueMessage[] MSGS = new IQueueMessage[EPHEMERAL_MAX_SIZE];
//        for (int i = 0; i < EPHEMERAL_MAX_SIZE; i++) {
//            MSGS[i] = queue.take();
//            assertNotNull(MSGS[i]);
//            assertEquals(EPHEMERAL_MAX_SIZE - i, queue.queueSize());
//            assertEquals(i + 1, queue.ephemeralSize());
//        }
//
//        boolean ephemeralIsFull = false;
//        try {
//            IQueueMessage msg = queue.take();
//            assertNull(msg);
//        } catch (QueueException.EphemeralIsFull e) {
//            ephemeralIsFull = true;
//        }
//        assertTrue(ephemeralIsFull);
//
//        for (int i = 0; i < EPHEMERAL_MAX_SIZE; i++) {
//            queue.finish(MSGS[i]);
//            assertEquals(1, queue.queueSize());
//            assertEquals(EPHEMERAL_MAX_SIZE - i - 1, queue.ephemeralSize());
//        }
//
//        IQueueMessage msg = queue.take();
//        assertNotNull(msg);
//        assertEquals(0, queue.queueSize());
//        assertEquals(1, queue.ephemeralSize());
//
//        queue.finish(msg);
//        assertEquals(0, queue.queueSize());
//        assertEquals(0, queue.ephemeralSize());
//    }
//}

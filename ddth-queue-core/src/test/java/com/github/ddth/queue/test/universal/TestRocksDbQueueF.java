package com.github.ddth.queue.test.universal;

import java.io.File;

import org.apache.commons.io.FileUtils;

import com.github.ddth.commons.utils.IdGenerator;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.RocksDbQueue;
import com.github.ddth.queue.impl.universal.UniversalQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalRocksDbQueue;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestRocksDbQueueF extends BaseTest {
    public TestRocksDbQueueF(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRocksDbQueueF.class);
    }

    @Override
    protected IQueue initQueueInstance() {
        File tempDir = FileUtils.getTempDirectory();
        File testDir = new File(tempDir, String.valueOf(System.currentTimeMillis()));
        UniversalRocksDbQueue queue = new UniversalRocksDbQueue();
        queue.setStorageDir(testDir.getAbsolutePath()).init();
        return queue;
    }

    @Override
    protected void destroyQueueInstance(IQueue queue) {
        if (queue instanceof RocksDbQueue) {
            File dir = new File(((RocksDbQueue) queue).getStorageDir());
            ((RocksDbQueue) queue).destroy();
            FileUtils.deleteQuietly(dir);
        } else {
            throw new RuntimeException("[queue] is not closed!");
        }
    }

    /*----------------------------------------------------------------------*/
    @org.junit.Test
    public void test1() throws Exception {
        assertNull(queue.take());
        assertEquals(0, queue.queueSize());
        assertEquals(0, queue.ephemeralSize());
    }

    @org.junit.Test
    public void test2() throws Exception {
        IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
        String content = idGen.generateId128Ascii();
        UniversalQueueMessage msg = UniversalQueueMessage.newInstance();
        msg.content(content);

        assertTrue(queue.queue(msg));
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.ephemeralSize());
    }

    @org.junit.Test
    public void test3() throws Exception {
        IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
        String content = idGen.generateId128Ascii();
        UniversalQueueMessage msg1 = UniversalQueueMessage.newInstance();
        msg1.content(content);

        assertTrue(queue.queue(msg1));
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.ephemeralSize());

        UniversalQueueMessage msg2 = (UniversalQueueMessage) queue.take();
        assertNotNull(msg2);
        assertEquals(content, msg2.contentAsString());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.ephemeralSize());
    }

    @org.junit.Test
    public void test4() throws Exception {
        IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
        String content = idGen.generateId128Ascii();
        UniversalQueueMessage msg1 = UniversalQueueMessage.newInstance();
        msg1.content(content);

        assertTrue(queue.queue(msg1));
        assertEquals(1, queue.queueSize());
        assertEquals(0, queue.ephemeralSize());

        UniversalQueueMessage msg2 = (UniversalQueueMessage) queue.take();
        assertNotNull(msg2);
        assertEquals(content, msg2.contentAsString());
        assertEquals(0, queue.queueSize());
        assertEquals(1, queue.ephemeralSize());

        queue.finish(msg2);
        assertNull(queue.take());
        assertEquals(0, queue.queueSize());
        assertEquals(0, queue.ephemeralSize());
    }
}

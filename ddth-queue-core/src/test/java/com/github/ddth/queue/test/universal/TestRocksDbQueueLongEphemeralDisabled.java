package com.github.ddth.queue.test.universal;

import java.io.File;

import org.apache.commons.io.FileUtils;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.RocksDbQueue;
import com.github.ddth.queue.impl.universal.idint.UniversalRocksDbQueue;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestRocksDbQueueLongEphemeralDisabled extends BaseQueueLongTest {
    public TestRocksDbQueueLongEphemeralDisabled(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRocksDbQueueLongEphemeralDisabled.class);
    }

    @Override
    protected IQueue initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsRocksDb") == null
                && System.getProperty("enableTestsRocksDB") == null) {
            return null;
        }
        File tempDir = FileUtils.getTempDirectory();
        File testDir = new File(tempDir, String.valueOf(System.currentTimeMillis()));
        UniversalRocksDbQueue queue = new UniversalRocksDbQueue();
        queue.setStorageDir(testDir.getAbsolutePath()).setEphemeralDisabled(true).init();
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

    protected int numTestMessages() {
        // to make a very long queue
        return 1024 * 1024;
    }

}

package com.github.ddth.queue.test.universal;

import java.io.File;

import org.apache.commons.io.FileUtils;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.RocksDbQueue;
import com.github.ddth.queue.impl.universal.UniversalRocksDbQueue;

import junit.framework.Test;
import junit.framework.TestSuite;

public class TestRocksDbQueue1 extends BaseQueueFunctionalTest {
    public TestRocksDbQueue1(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRocksDbQueue1.class);
    }

    protected IQueue initQueueInstance(int ephemeralMaxSize) throws Exception {
        if (System.getProperty("enableTestsRocksDb") == null
                && System.getProperty("enableTestsRocksDB") == null) {
            return null;
        }
        File tempDir = FileUtils.getTempDirectory();
        File testDir = new File(tempDir, String.valueOf(System.currentTimeMillis()));
        UniversalRocksDbQueue queue = new UniversalRocksDbQueue();
        queue.setStorageDir(testDir.getAbsolutePath()).setEphemeralDisabled(false)
                .setEphemeralMaxSize(ephemeralMaxSize).init();
        return queue;
    }

    protected void destroyQueueInstance(IQueue queue) {
        if (queue instanceof RocksDbQueue) {
            ((RocksDbQueue) queue).destroy();
        } else {
            throw new RuntimeException("[queue] is not closed!");
        }
    }

}

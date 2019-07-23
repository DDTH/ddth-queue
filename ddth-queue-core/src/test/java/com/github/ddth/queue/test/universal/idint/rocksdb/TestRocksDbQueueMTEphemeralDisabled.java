package com.github.ddth.queue.test.universal.idint.rocksdb;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.RocksDbQueue;
import com.github.ddth.queue.impl.universal.idint.UniversalRocksDbQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.commons.io.FileUtils;

import java.io.File;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.rocksdb.TestRocksDbQueueMTEphemeralDisabled -DenableTestsRocksDb=true
 */

public class TestRocksDbQueueMTEphemeralDisabled extends BaseQueueMultiThreadsTest<Long> {
    public TestRocksDbQueueMTEphemeralDisabled(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRocksDbQueueMTEphemeralDisabled.class);
    }

    @Override
    protected IQueue<Long, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsRocksDb") == null && System.getProperty("enableTestsRocksDB") == null) {
            return null;
        }
        File tempDir = FileUtils.getTempDirectory();
        File testDir = new File(tempDir, String.valueOf(System.currentTimeMillis()));
        RocksDbQueue<Long, byte[]> queue = new UniversalRocksDbQueue() {
            public void destroy() {
                try {
                    super.destroy();
                } finally {
                    FileUtils.deleteQuietly(testDir);
                }
            }
        };
        queue.setStorageDir(testDir.getAbsolutePath()).setEphemeralDisabled(true).init();
        return queue;
    }

    protected int numTestMessages() {
        return 512 * 1024;
    }
}

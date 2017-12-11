package com.github.ddth.queue.test.universal.idint.rocksdb;

import java.io.File;

import org.apache.commons.io.FileUtils;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.NoopQueueObserver;
import com.github.ddth.queue.impl.RocksDbQueue;
import com.github.ddth.queue.impl.universal.idint.UniversalRocksDbQueue;
import com.github.ddth.queue.test.universal.BaseQueueLongTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.rocksdb.TestRocksDbQueueLong -DenableTestsRocksDb=true
 */

public class TestRocksDbQueueLong extends BaseQueueLongTest<Long> {
    public TestRocksDbQueueLong(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestRocksDbQueueLong.class);
    }

    @Override
    protected IQueue<Long, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsRocksDb") == null
                && System.getProperty("enableTestsRocksDB") == null) {
            return null;
        }
        File tempDir = FileUtils.getTempDirectory();
        File testDir = new File(tempDir, String.valueOf(System.currentTimeMillis()));
        RocksDbQueue<Long, byte[]> queue = new UniversalRocksDbQueue();
        queue.setObserver(new NoopQueueObserver<Long, byte[]>() {
            @Override
            public void postDestroy(IQueue<Long, byte[]> queue) {
                FileUtils.deleteQuietly(testDir);
            }
        });
        queue.setStorageDir(testDir.getAbsolutePath()).setEphemeralDisabled(false).init();
        return queue;
    }

    protected int numTestMessages() {
        // to make a very long queue
        return 1024 * 1024;
    }

}

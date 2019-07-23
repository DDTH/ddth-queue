package com.github.ddth.queue.qnd.rocksdb;

import java.io.File;

import org.apache.commons.io.FileUtils;

import com.github.ddth.commons.rocksdb.RocksDbWrapper;
import com.github.ddth.queue.internal.utils.QueueUtils;

public class QndRocksDbMultiThreads {
    public static void main(String[] args) throws Exception {
        File storageDir = new File("/tmp/rocksdb");
        FileUtils.deleteQuietly(storageDir);
        storageDir.mkdirs();

        final int NUM_THREADS = 4;
        final int NUM_ITEMS_PER_THREAD = 100000;
        final Thread[] THREADS = new Thread[NUM_THREADS];
        final String[] CF_LIST = new String[NUM_THREADS];
        for (int i = 0; i < NUM_THREADS; i++) {
            CF_LIST[i] = "CF" + i;
        }

        try (RocksDbWrapper rocskDb = RocksDbWrapper.openReadWrite(storageDir, CF_LIST)) {
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < NUM_THREADS; i++) {
                final int tid = i;
                Thread t = new Thread() {
                    public void run() {
                        final String cfName = "CF" + tid;
                        for (int i = 0; i < NUM_ITEMS_PER_THREAD; i++) {
                            String key = QueueUtils.IDGEN.generateId128Hex();
                            String value = String.valueOf(i + 1);
                            rocskDb.put(cfName, key, value);
                        }
                    }
                };
                t.start();
                THREADS[i] = t;
            }
            for (int i = 0; i < NUM_THREADS; i++) {
                THREADS[i].join();
            }
            long t2 = System.currentTimeMillis();
            long d = t2 - t1;
            long total = (long) NUM_THREADS * (long) NUM_ITEMS_PER_THREAD;
            System.out.println(
                    "Wrote " + total + " in " + d + " ms (" + (total * 1000.0 / d) + "items/sec");
        }

        try (RocksDbWrapper rocskDbReadonly = RocksDbWrapper.openReadOnly(storageDir)) {
            System.out.println(rocskDbReadonly.getProperty("default", "rocksdb.estimate-num-keys"));
            System.out.println(rocskDbReadonly.getEstimateNumKeys("CF0"));
            System.out.println(rocskDbReadonly.getEstimateNumKeys("CF1"));
            System.out.println(rocskDbReadonly.getEstimateNumKeys("CF2"));
            System.out.println(rocskDbReadonly.getEstimateNumKeys("CF3"));
            System.out.println(rocskDbReadonly.getEstimateNumKeys("CF4"));
        }
    }
}

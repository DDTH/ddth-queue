package com.github.ddth.queue.qnd.rocksdb;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.rocksdb.DBOptions;

import com.github.ddth.commons.rocksdb.RocksDbUtils;
import com.github.ddth.commons.rocksdb.RocksDbWrapper;
import com.github.ddth.queue.internal.utils.QueueUtils;

public class QndRocksDb1milRecords {
    public static void main(String[] args) throws Exception {
        File storageDir = new File("/tmp/rocksdb");
        FileUtils.deleteQuietly(storageDir);
        storageDir.mkdirs();

        DBOptions dbOptions = RocksDbUtils.defaultDbOptions().setMaxTotalWalSize(16);

        try (RocksDbWrapper rocksDbWrapper = RocksDbWrapper.openReadWrite(storageDir, dbOptions,
                null, null, (String[]) null)) {
            System.out.println("ColumnFamilies: " + rocksDbWrapper.getColumnFamilyNames());

            final int NUM_ITEMS = 1000000;

            String cfName = RocksDbWrapper.DEFAULT_COLUMN_FAMILY;
            // ColumnFamilyHandle cfh = rocskDb.getColumnFamilyHandle(cfName);
            long t1 = System.currentTimeMillis();
            for (int i = 0; i < NUM_ITEMS; i++) {
                String key = QueueUtils.IDGEN.generateId128Hex();
                String value = String.valueOf(i + 1);
                rocksDbWrapper.put(cfName, key, value);
            }
            long t2 = System.currentTimeMillis();
            long d = t2 - t1;
            System.out.println("Wrote " + NUM_ITEMS + " in " + d + " ms ("
                    + Math.round(NUM_ITEMS * 1000.0 / d) + " items/sec)");

            t1 = System.currentTimeMillis();
            rocksDbWrapper.compactRange();
            System.out.println(
                    "Compact Range finished in " + (System.currentTimeMillis() - t1) + "ms");

            System.out.println("Num keys: " + rocksDbWrapper.getEstimateNumKeys("default"));
            System.out.println(rocksDbWrapper.getProperty("default", "rocksdb.stats"));
        }

        try (RocksDbWrapper rocksDbReadonly = RocksDbWrapper.openReadOnly(storageDir)) {
            System.out.println("Num keys: " + rocksDbReadonly.getEstimateNumKeys("default"));
        }
    }
}

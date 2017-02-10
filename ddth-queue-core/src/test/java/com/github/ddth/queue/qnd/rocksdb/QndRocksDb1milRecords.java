package com.github.ddth.queue.qnd.rocksdb;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.rocksdb.DBOptions;

import com.github.ddth.queue.impl.rocksdb.RocksDbUtils;
import com.github.ddth.queue.impl.rocksdb.RocksDbWrapper;
import com.github.ddth.queue.utils.QueueUtils;

public class QndRocksDb1milRecords {

    // private static void _write(WriteBatch batch) throws RocksDBException {
    // try {
    // rocksDb.write(writeOptions, batch);
    // } finally {
    // }
    // }
    //
    // private static String put(String value) throws RocksDBException {
    // String key = idGen.generateId128Hex().toLowerCase();
    // WriteBatch writeBatch = new WriteBatch();
    // try {
    // writeBatch.put(key.getBytes(), value.getBytes());
    // _write(writeBatch);
    // return key;
    // } finally {
    // writeBatch.close();
    // }
    // }

    // private static void remove(String key) throws RocksDBException {
    // WriteBatch writeBatch = new WriteBatch();
    // try {
    // writeBatch.remove(key.getBytes());
    // _write(writeBatch);
    // } finally {
    // writeBatch.close();
    // }
    // }

    // private static byte[][] poll(RocksIterator it) {
    // if (!it.isValid()) {
    // it.seekToFirst();
    // }
    // if (!it.isValid()) {
    // return null;
    // }
    // byte[][] result = new byte[2][];
    // result[0] = it.key();
    // result[1] = it.value();
    // it.next();
    // return result;
    // }

    // private static void printRecord(byte[][] record) {
    // if (record == null) {
    // System.out.println("Record: null");
    // } else {
    // System.out.println(
    // "Record: {" + new String(record[0]) + ": " + new String(record[1]) +
    // "}");
    // }
    // }

    public static void main(String[] args) throws Exception {
        File storageDir = new File("/tmp/rocksdb");
        FileUtils.deleteQuietly(storageDir);
        storageDir.mkdirs();

        DBOptions dbOptions = RocksDbUtils.buildDbOptions().setMaxTotalWalSize(16);

        try (RocksDbWrapper rocksDbWrapper = RocksDbWrapper.openReadWrite(storageDir, dbOptions,
                null, null, null)) {
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

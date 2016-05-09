package com.github.ddth.queue.qnd.rocksdb;

import java.io.File;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;

import com.github.ddth.commons.utils.IdGenerator;

public class QndRocksDb {

    private static String storageDir = "/tmp/rocksdb";
    private static RocksDB rocksDb;
    private static Options rocksOptions;
    private static WriteOptions writeOptions;
    private static ReadOptions readOptions;
    private static IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());

    private static void init() throws Exception {
        File STORAGE_DIR = new File(storageDir);
        FileUtils.forceMkdir(STORAGE_DIR);

        RocksDB.loadLibrary();

        rocksOptions = new Options();
        rocksOptions.setCreateIfMissing(true).getEnv().setBackgroundThreads(1, Env.FLUSH_POOL)
                .setBackgroundThreads(4, Env.COMPACTION_POOL);
        rocksOptions.setMaxBackgroundFlushes(1).setMaxBackgroundCompactions(4);
        rocksOptions.setWriteBufferSize(32 * 1024L * 1024L).setMinWriteBufferNumberToMerge(2)
                .setLevelZeroFileNumCompactionTrigger(512).setTargetFileSizeBase(16 * 1024 * 1024);

        rocksOptions.setMemTableConfig(new SkipListMemTableConfig());
        // rocksOptions.setMemTableConfig(new HashSkipListMemTableConfig());
        // rocksOptions.setMemTableConfig(new HashLinkedListMemTableConfig());

        writeOptions = new WriteOptions().setSync(false).setDisableWAL(false);

        readOptions = new ReadOptions();
        readOptions.setTailing(true);

        try {
            rocksDb = RocksDB.open(rocksOptions, STORAGE_DIR.getAbsolutePath());
        } catch (RocksDBException e) {
            destroy();
            throw new RuntimeException(e);
        }
    }

    public static void destroy() {
        if (rocksDb != null) {
            rocksDb.close();
        }

        if (rocksOptions != null) {
            rocksOptions.dispose();
        }

        if (writeOptions != null) {
            writeOptions.dispose();
        }

        if (readOptions != null) {
            readOptions.dispose();
        }
    }

    private static void _write(WriteBatch batch) throws RocksDBException {
        try {
            rocksDb.write(writeOptions, batch);
        } finally {
        }
    }

    private static String put(String value) throws RocksDBException {
        String key = idGen.generateId128Hex().toLowerCase();
        WriteBatch writeBatch = new WriteBatch();
        try {
            writeBatch.put(key.getBytes(), value.getBytes());
            _write(writeBatch);
            return key;
        } finally {
            writeBatch.dispose();
        }
    }

    private static void remove(String key) throws RocksDBException {
        WriteBatch writeBatch = new WriteBatch();
        try {
            writeBatch.remove(key.getBytes());
            _write(writeBatch);
        } finally {
            writeBatch.dispose();
        }
    }

    private static byte[][] poll(RocksIterator it) {
        if (!it.isValid()) {
            it.seekToFirst();
        }
        if (!it.isValid()) {
            return null;
        }
        byte[][] result = new byte[2][];
        result[0] = it.key();
        result[1] = it.value();
        it.next();
        return result;
    }

    private static void printRecord(byte[][] record) {
        if (record == null) {
            System.out.println("Record: null");
        } else {
            System.out.println(
                    "Record: {" + new String(record[0]) + ": " + new String(record[1]) + "}");
        }
    }

    public static void main(String[] args) throws Exception {
        init();

        Options options = new Options();
        try {
            List<byte[]> cfList = RocksDB.listColumnFamilies(options, storageDir);
            for (byte[] cf : cfList) {
                System.out.println(new String(cf));
            }
        } finally {
            options.dispose();
        }

        RocksIterator it = rocksDb.newIterator(readOptions);
        byte[][] record = poll(it);
        printRecord(record);

        String key = put("1");
        record = poll(it);
        printRecord(record);

        remove(key);
        record = poll(it);
        printRecord(record);

        key = put("2");
        record = poll(it);
        printRecord(record);

        it.dispose();
        destroy();
    }

}

package com.github.ddth.queue.qnd.rocksdb;

import java.io.File;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.FileUtils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.MergeOperator;
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
    private static DBOptions dbOptions;
    private static WriteOptions writeOptions;
    private static ReadOptions readOptions;
    private static IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
    private static ColumnFamilyHandle cfMetadata;

    private static void init() throws Exception {
        File STORAGE_DIR = new File(storageDir);
        FileUtils.deleteQuietly(STORAGE_DIR);
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

        dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
        dbOptions.setMaxBackgroundCompactions(2).setMaxBackgroundFlushes(2);
        dbOptions.setAllowMmapReads(true).setAllowMmapWrites(true).setAllowOsBuffer(true);
        dbOptions.setIncreaseParallelism(Runtime.getRuntime().availableProcessors());

        writeOptions = new WriteOptions().setSync(false).setDisableWAL(false);

        readOptions = new ReadOptions();
        readOptions.setTailing(true);

        try {
            ColumnFamilyOptions cfOptions = new ColumnFamilyOptions();
            cfOptions.setMergeOperatorName("uint64add");
            cfOptions.setMergeOperator(new MergeOperator() {
                @Override
                public long newMergeOperatorHandle() {
                    long value = 0;
                    System.out.println("===" + value);
                    return value;
                }
            });
            ColumnFamilyDescriptor cfDesc = new ColumnFamilyDescriptor("metadata".getBytes(),
                    cfOptions);

            List<ColumnFamilyDescriptor> cfdList = new ArrayList<>();
            cfdList.add(cfDesc);
            cfdList.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

            List<ColumnFamilyHandle> cfhList = new ArrayList<>();
            // rocksDb = RocksDB.open(rocksOptions,
            // STORAGE_DIR.getAbsolutePath(), cfdList, null);
            rocksDb = RocksDB.open(dbOptions, STORAGE_DIR.getAbsolutePath(), cfdList, cfhList);
            cfMetadata = cfhList.get(0);
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
        try {
            Options options = new Options();
            try {
                List<byte[]> cfList = RocksDB.listColumnFamilies(options, storageDir);
                for (byte[] cf : cfList) {
                    System.out.println(new String(cf));
                }
            } finally {
                options.dispose();
            }

            byte[] key = "key".getBytes();
            byte[] value = ByteBuffer.allocate(8).putLong(1).array();
            byte[] data;
            rocksDb.put(cfMetadata, key, value);
            data = rocksDb.get(cfMetadata, key);
            System.out.println(data.length + " / " + new String(data) + " / "
                    + ByteBuffer.wrap(data).getLong());

            byte[] value2 = ByteBuffer.allocate(8).putLong(2).array();
            rocksDb.merge(cfMetadata, key, value2);
            data = rocksDb.get(cfMetadata, key);
            System.out.println(data.length + " / " + new String(data) + " / "
                    + ByteBuffer.wrap(data).getLong());

            byte[] value3 = ByteBuffer.allocate(8).putLong(-2).array();
            rocksDb.merge(cfMetadata, key, value3);
            data = rocksDb.get(cfMetadata, key);
            System.out.println(data.length + " / " + new String(data) + " / "
                    + ByteBuffer.wrap(data).getLong());

            // RocksIterator it = rocksDb.newIterator(readOptions);
            // byte[][] record = poll(it);
            // printRecord(record);
            //
            // String key = put("1");
            // record = poll(it);
            // printRecord(record);
            //
            // remove(key);
            // record = poll(it);
            // printRecord(record);
            //
            // key = put("2");
            // record = poll(it);
            // printRecord(record);

            // it.dispose();
        } finally {
            destroy();
        }
    }

}

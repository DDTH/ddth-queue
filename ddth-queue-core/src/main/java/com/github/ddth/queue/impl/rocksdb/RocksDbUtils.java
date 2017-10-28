package com.github.ddth.queue.impl.rocksdb;

import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksObject;
import org.rocksdb.SkipListMemTableConfig;
import org.rocksdb.WriteOptions;

import com.github.ddth.queue.utils.QueueUtils;

/**
 * RocksDB Utility class.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.5.0
 */
public class RocksDbUtils {
    public final static Charset UTF8 = Charset.forName("UTF-8");

    /**
     * Builds default RocksDb Options.
     * 
     * @return
     */
    public static Options buildOptions() {
        return buildOptions(4, 128, 8L * 1024L * 1024, 32L * 1024L * 1024L);
    }

    /**
     * Builds RocksDb Options.
     * 
     * @param maxBackgroundThreads
     * @param levelZeloFileNumCompactionTrigger
     * @param writeBufferSize
     * @param targetFileSizeBase
     * @return
     */
    public static Options buildOptions(int maxBackgroundThreads,
            int levelZeloFileNumCompactionTrigger, long writeBufferSize, long targetFileSizeBase) {
        Options rocksOptions = new Options();
        rocksOptions.setCreateIfMissing(true).getEnv().setBackgroundThreads(1, Env.FLUSH_POOL)
                .setBackgroundThreads(maxBackgroundThreads, Env.COMPACTION_POOL);
        rocksOptions.setMaxBackgroundFlushes(1).setMaxBackgroundCompactions(maxBackgroundThreads);
        rocksOptions.setWriteBufferSize(writeBufferSize).setMinWriteBufferNumberToMerge(2)
                .setLevelZeroFileNumCompactionTrigger(levelZeloFileNumCompactionTrigger)
                .setTargetFileSizeBase(targetFileSizeBase);

        rocksOptions.setMemTableConfig(new SkipListMemTableConfig());
        // rocksOptions.setMemTableConfig(new HashSkipListMemTableConfig());
        // rocksOptions.setMemTableConfig(new HashLinkedListMemTableConfig());

        return rocksOptions;
    }

    /**
     * Builds default RocskDb DBOptions.
     * 
     * @return
     */
    public static DBOptions buildDbOptions() {
        return buildDbOptions(2, 4, 8,
                0 /* write all log to one file and roll by time */);
    }

    /**
     * Builds RocksDb DBOptions.
     * 
     * @param maxBackgroundFlushes
     *            high priority threads, usually 1 or 2 would be enough
     * @param maxBackgroundCompactions
     *            low priority threads, between 1 - num_cpu_cores
     * @param maxBackgroundThreads
     *            {@code >= maxBackgroundFlushes, maxBackgroundCompactions}
     * @param maxLogFileSize
     * @return
     */
    public static DBOptions buildDbOptions(int maxBackgroundFlushes, int maxBackgroundCompactions,
            int maxBackgroundThreads, long maxLogFileSize) {
        DBOptions dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
                .setErrorIfExists(false);
        dbOptions.setMaxBackgroundFlushes(maxBackgroundFlushes)
                .setMaxBackgroundCompactions(maxBackgroundCompactions)
                .setIncreaseParallelism(maxBackgroundThreads);
        dbOptions.setAllowMmapReads(true).setAllowMmapWrites(true);
        dbOptions.setMaxOpenFiles(-1);
        dbOptions.setKeepLogFileNum(100).setLogFileTimeToRoll(3600)
                .setMaxLogFileSize(maxLogFileSize);
        return dbOptions;
    }

    /**
     * Builds default RocskDb WriteOptions.
     * 
     * @return
     */
    public static WriteOptions buildWriteOptions() {
        /*
         * As of RocksDB v3.10.1, no data lost if process crashes even if
         * sync==false. Data lost only when server/OS crashes. So it's
         * relatively safe to set sync=false for fast write.
         */
        return buildWriteOptions(false, false);
    }

    /**
     * Builds RocksDb WriteOptions.
     * 
     * @param sync
     * @param disableWAL
     * @return
     */
    public static WriteOptions buildWriteOptions(boolean sync, boolean disableWAL) {
        WriteOptions writeOptions = new WriteOptions();
        writeOptions.setSync(sync).setDisableWAL(disableWAL);
        return writeOptions;
    }

    /**
     * Builds default RocskDb ReadOptions.
     * 
     * @return
     */
    public static ReadOptions buildReadOptions() {
        return buildReadOptions(true);
    }

    /**
     * Builds RocksDb ReadOptions.
     * 
     * @param tailing
     * @return
     */
    public static ReadOptions buildReadOptions(boolean tailing) {
        ReadOptions readOptions = new ReadOptions();
        readOptions.setTailing(tailing);
        return readOptions;
    }

    /**
     * Silently close RocksDb objects.
     * 
     * @param rocksObjList
     */
    public static void closeRocksObjects(RocksObject... rocksObjList) {
        for (RocksObject obj : rocksObjList) {
            try {
                if (obj != null)
                    obj.close();
            } catch (Exception e) {
            }
        }
    }

    /**
     * Gets all available column family names.
     * 
     * @param path
     * @return
     * @throws RocksDBException
     */
    public static String[] getColumnFamilyList(String path) throws RocksDBException {
        List<byte[]> cfList = RocksDB.listColumnFamilies(new Options(), path);
        if (cfList == null || cfList.size() == 0) {
            return ArrayUtils.EMPTY_STRING_ARRAY;
        }
        List<String> result = new ArrayList<>(cfList.size());
        for (byte[] cf : cfList) {
            result.add(new String(cf, QueueUtils.UTF8));
        }
        return result.toArray(ArrayUtils.EMPTY_STRING_ARRAY);
    }

    /**
     * Builds a {@link ColumnFamilyDescriptor} with default options.
     * 
     * @param cfName
     * @return
     */
    public static ColumnFamilyDescriptor buildColumnFamilyDescriptor(String cfName) {
        return buildColumnFamilyDescriptor(null, cfName);
    }

    /**
     * Builds a {@link ColumnFamilyDescriptor}, specifying options.
     * 
     * @param cfOptions
     * @param cfName
     * @return
     */
    public static ColumnFamilyDescriptor buildColumnFamilyDescriptor(ColumnFamilyOptions cfOptions,
            String cfName) {
        return cfOptions != null
                ? new ColumnFamilyDescriptor(cfName.getBytes(QueueUtils.UTF8), cfOptions)
                : new ColumnFamilyDescriptor(cfName.getBytes(QueueUtils.UTF8));
    }

    /**
     * Builds a list of {@link ColumnFamilyDescriptor}s with default options.
     * 
     * @param cfNames
     * @return
     */
    public static List<ColumnFamilyDescriptor> buildColumnFamilyDescriptors(String... cfNames) {
        return buildColumnFamilyDescriptors(null, cfNames);
    }

    /**
     * Builds a list of {@link ColumnFamilyDescriptor}s, specifying options.
     * 
     * @param cfOptions
     * @param cfNames
     * @return
     */
    public static List<ColumnFamilyDescriptor> buildColumnFamilyDescriptors(
            ColumnFamilyOptions cfOptions, String... cfNames) {
        List<ColumnFamilyDescriptor> result = new ArrayList<>();
        if (cfNames != null) {
            for (String cfName : cfNames) {
                result.add(buildColumnFamilyDescriptor(cfOptions, cfName));
            }
        }
        return result;
    }
}

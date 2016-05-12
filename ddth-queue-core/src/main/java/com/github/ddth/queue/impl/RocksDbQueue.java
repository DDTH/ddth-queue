package com.github.ddth.queue.impl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksObject;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.commons.utils.IdGenerator;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;

/**
 * RocksDB implementation of {@link IQueue}.
 * 
 * <p>
 * Implementation:
 * <ul>
 * <li>RocksDB as queue storage</li>
 * </ul>
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.0
 */
public abstract class RocksDbQueue implements IQueue, Closeable, AutoCloseable {

    private Logger LOGGER = LoggerFactory.getLogger(RocksDbQueue.class);

    private String storageDir = "/tmp/ddth-rocksdb-queue";
    private RocksDB rocksDb;
    private RocksIterator itQueue;
    // private Options rocksOptions;
    private DBOptions dbOptions;
    private WriteOptions writeOptions;
    private ReadOptions readOptions;
    private IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
    private byte[] lastFetchedId = null;
    private Lock lockPut = new ReentrantLock(), lockTake = new ReentrantLock();

    private boolean ephemeralDisabled = false;

    private WriteBatch batchPutToQueue, batchTake;

    private String cfNameQueue = "queue", cfNameMetadata = "metadata",
            cfNameEphemeral = "ephemeral";
    private ColumnFamilyHandle cfQueue, cfMetadata, cfEphemeral;
    private List<ColumnFamilyHandle> cfHandleList = new ArrayList<>();

    /**
     * @return
     * @since 0.4.0.1
     */
    protected RocksDB getRocksDb() {
        return rocksDb;
    }

    /**
     * @return
     * @since 0.4.0.1
     */
    protected ColumnFamilyHandle getCfQueue() {
        return cfQueue;
    }

    /**
     * @return
     * @since 0.4.0.1
     */
    protected ColumnFamilyHandle getCfMetadata() {
        return cfMetadata;
    }

    /**
     * @return
     * @since 0.4.0.1
     */
    protected ColumnFamilyHandle getCfEphemeral() {
        return cfEphemeral;
    }

    /**
     * @return
     * @since 0.4.0.1
     */
    protected ReadOptions getReadOptions() {
        return readOptions;
    }

    /**
     * @return
     * @since 0.4.0.1
     */
    protected WriteOptions getWriteOptions() {
        return writeOptions;
    }

    /**
     * Is ephemeral storage disabled?
     * 
     * @return
     * @since 0.4.0.1
     */
    public boolean getEphemeralDisabled() {
        return ephemeralDisabled;
    }

    /**
     * Is ephemeral storage disabled?
     * 
     * @return
     * @since 0.4.0.1
     */
    public boolean isEphemeralDisabled() {
        return ephemeralDisabled;
    }

    /**
     * Disables/Enables ephemeral storage.
     * 
     * @param ephemeralDisabled
     *            {@code true} to disable ephemeral storage, {@code false}
     *            otherwise.
     * @return
     * @since 0.4.0.1
     */
    public RocksDbQueue setEphemeralDisabled(boolean ephemeralDisabled) {
        this.ephemeralDisabled = ephemeralDisabled;
        return this;
    }

    /**
     * RocksDB's storage directory.
     * 
     * @return
     */
    public String getStorageDir() {
        return storageDir;
    }

    /**
     * Sets RocksDB's storage directory.
     * 
     * @param storageDir
     * @return
     */
    public RocksDbQueue setStorageDir(String storageDir) {
        this.storageDir = storageDir;
        return this;
    }

    /**
     * Name of the ColumnFamily to store queue messages.
     * 
     * @return
     * @since 0.4.0.1
     */
    public String getCfNameQueue() {
        return cfNameQueue;
    }

    /**
     * Sets name of the ColumnFamily to store queue messages.
     * 
     * @param cfNameQueue
     * @return
     * @since 0.4.0.1
     */
    public RocksDbQueue setCfNameQueue(String cfNameQueue) {
        this.cfNameQueue = cfNameQueue;
        return this;
    }

    /**
     * Name of the ColumnFamily to store metadata.
     * 
     * @return
     * @since 0.4.0.1
     */
    public String getCfNameMetadata() {
        return cfNameMetadata;
    }

    /**
     * Sets name of the ColumnFamily to store metadata.
     * 
     * @param cfNameMetadata
     * @return
     * @since 0.4.0.1
     */
    public RocksDbQueue setCfNameMetadata(String cfNameMetadata) {
        this.cfNameMetadata = cfNameMetadata;
        return this;
    }

    /**
     * Name of the ColumnFamily to store ephemeral messages.
     * 
     * @return
     * @since 0.4.0.1
     */
    public String getCfNameEphemeral() {
        return cfNameEphemeral;
    }

    /**
     * Sets name of the ColumnFamily to store ephemeral messages.
     * 
     * @param cfNameEphemeral
     * @return
     * @since 0.4.0.1
     */
    public RocksDbQueue setCfNameEphemeral(String cfNameEphemeral) {
        this.cfNameEphemeral = cfNameEphemeral;
        return this;
    }

    /*----------------------------------------------------------------------*/

    /**
     * Init method.
     * 
     * @return
     */
    public RocksDbQueue init() {
        File STORAGE_DIR = new File(storageDir);
        LOGGER.info("Storage Directory: " + STORAGE_DIR.getAbsolutePath());
        try {
            FileUtils.forceMkdir(STORAGE_DIR);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        RocksDB.loadLibrary();

        batchPutToQueue = new WriteBatch();
        batchTake = new WriteBatch();

        // rocksOptions = new Options();
        // rocksOptions.setCreateIfMissing(true).getEnv().setBackgroundThreads(2,
        // Env.FLUSH_POOL)
        // .setBackgroundThreads(4, Env.COMPACTION_POOL);
        // rocksOptions.setMaxBackgroundFlushes(2).setMaxBackgroundCompactions(4);
        // rocksOptions.setWriteBufferSize(8 * 1024L *
        // 1024L).setMinWriteBufferNumberToMerge(2)
        // .setLevelZeroFileNumCompactionTrigger(512).setTargetFileSizeBase(16 *
        // 1024 * 1024);
        //
        // rocksOptions.setMemTableConfig(new SkipListMemTableConfig());
        // // rocksOptions.setMemTableConfig(new HashSkipListMemTableConfig());
        // // rocksOptions.setMemTableConfig(new
        // HashLinkedListMemTableConfig());

        dbOptions = new DBOptions();
        dbOptions.setCreateIfMissing(true).setCreateMissingColumnFamilies(true)
                .setMaxBackgroundFlushes(2).setMaxBackgroundCompactions(2);
        dbOptions.setAllowMmapReads(true).setAllowMmapWrites(true).setAllowOsBuffer(true);

        /*
         * As of RocksDB v3.10.1, no data lost if process crashes even if
         * sync==false. Data lost only when server/OS crashes. So it's
         * relatively safe to set sync=false for fast write.
         */
        writeOptions = new WriteOptions().setSync(false).setDisableWAL(false);

        readOptions = new ReadOptions().setTailing(true);

        try {
            List<ColumnFamilyDescriptor> cfDescList = new ArrayList<>();
            cfDescList.add(new ColumnFamilyDescriptor(cfNameEphemeral.getBytes()));
            cfDescList.add(new ColumnFamilyDescriptor(cfNameMetadata.getBytes()));
            cfDescList.add(new ColumnFamilyDescriptor(cfNameQueue.getBytes()));
            cfDescList.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));

            rocksDb = RocksDB.open(dbOptions, STORAGE_DIR.getAbsolutePath(), cfDescList,
                    cfHandleList);

            cfEphemeral = cfHandleList.get(0);
            cfMetadata = cfHandleList.get(1);
            cfQueue = cfHandleList.get(2);

            itQueue = rocksDb.newIterator(cfQueue, readOptions);
            lastFetchedId = loadLastFetchedId();
        } catch (RocksDBException e) {
            destroy();
            throw new RuntimeException(e);
        }

        return this;
    }

    private void disposeRocksObject(RocksObject ro) {
        if (ro != null) {
            try {
                ro.dispose();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        try {
            saveLastFetchedId(lastFetchedId);
        } catch (RocksDBException e) {
            LOGGER.error(e.getMessage(), e);
        }

        disposeRocksObject(batchPutToQueue);
        disposeRocksObject(batchTake);

        disposeRocksObject(cfEphemeral);
        disposeRocksObject(cfMetadata);
        disposeRocksObject(cfQueue);

        disposeRocksObject(itQueue);

        disposeRocksObject(rocksDb);
        // disposeRocksObject(rocksOptions);
        disposeRocksObject(dbOptions);
        disposeRocksObject(readOptions);
        disposeRocksObject(writeOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        destroy();
    }

    private final static byte[] keyLastFetchedId = "last-fetched-id".getBytes();

    /**
     * Loads last saved last-fetched-id.
     * 
     * @return
     * @since 0.4.0.1
     * @throws RocksDBException
     */
    private byte[] loadLastFetchedId() throws RocksDBException {
        return rocksDb.get(cfMetadata, readOptions, keyLastFetchedId);
    }

    /**
     * Saves last-fetched-id.
     * 
     * @param lastFetchedId
     * @since 0.4.0.1
     * @throws RocksDBException
     */
    private void saveLastFetchedId(byte[] lastFetchedId) throws RocksDBException {
        if (lastFetchedId != null) {
            rocksDb.put(cfMetadata, writeOptions, keyLastFetchedId, lastFetchedId);
        }
    }

    /**
     * Serializes a queue message to byte[].
     * 
     * @param msg
     * @return
     */
    protected abstract byte[] serialize(IQueueMessage msg);

    /**
     * Deserilizes a queue message.
     * 
     * @param msgData
     * @return
     */
    protected abstract IQueueMessage deserialize(byte[] msgData);

    protected boolean putToQueue(IQueueMessage msg, boolean removeFromEphemeral) {
        byte[] value = serialize(msg);
        lockPut.lock();
        try {
            byte[] key = idGen.generateId128Hex().toLowerCase().getBytes();
            try {
                try {
                    batchPutToQueue.put(cfQueue, key, value);
                    if (removeFromEphemeral && !ephemeralDisabled) {
                        byte[] _key = msg.qId().toString().getBytes();
                        batchPutToQueue.remove(cfEphemeral, _key);
                    }
                    rocksDb.write(writeOptions, batchPutToQueue);
                } finally {
                    batchPutToQueue.clear();
                }
                return true;
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        } finally {
            lockPut.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean queue(IQueueMessage _msg) {
        IQueueMessage msg = _msg.clone();
        Date now = new Date();
        msg.qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now);
        return putToQueue(msg, false);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeue(IQueueMessage _msg) {
        IQueueMessage msg = _msg.clone();
        Date now = new Date();
        msg.qIncNumRequeues().qTimestamp(now);
        return putToQueue(msg, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeueSilent(IQueueMessage msg) {
        return putToQueue(msg, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage msg) {
        if (!ephemeralDisabled) {
            byte[] key = msg.qId().toString().getBytes();
            try {
                rocksDb.remove(cfEphemeral, writeOptions, key);
            } catch (RocksDBException e) {
                throw new RuntimeException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IQueueMessage take() {
        lockTake.lock();
        try {
            if (lastFetchedId == null) {
                itQueue.seekToFirst();
            } else {
                itQueue.seek(lastFetchedId);
            }
            if (!itQueue.isValid()) {
                return null;
            }
            lastFetchedId = itQueue.key();
            byte[] value = itQueue.value();
            IQueueMessage msg = deserialize(value);
            try {
                try {
                    batchTake.remove(cfQueue, lastFetchedId);
                    batchTake.put(cfMetadata, keyLastFetchedId, lastFetchedId);
                    if (!ephemeralDisabled && msg != null) {
                        byte[] _key = msg.qId().toString().getBytes();
                        batchTake.put(cfEphemeral, _key, value);
                    }
                    rocksDb.write(writeOptions, batchTake);
                } finally {
                    batchTake.clear();
                }
            } catch (RocksDBException e) {
                LOGGER.error(e.getMessage(), e);
            }
            itQueue.next();
            return msg;
        } finally {
            lockTake.unlock();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @param thresholdTimestampMs
     * @return
     */
    @Override
    public Collection<IQueueMessage> getOrphanMessages(long thresholdTimestampMs) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean moveFromEphemeralToQueueStorage(IQueueMessage msg) {
        return putToQueue(msg, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    synchronized public int queueSize() {
        RocksIterator it = getRocksDb().newIterator(cfQueue, readOptions);
        try {
            int count = 0;
            it.seekToFirst();
            while (it.isValid()) {
                count++;
                it.next();
            }
            return count;
        } finally {
            it.dispose();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    synchronized public int ephemeralSize() {
        RocksIterator it = getRocksDb().newIterator(getCfEphemeral(), getReadOptions());
        try {
            int count = 0;
            it.seekToFirst();
            while (it.isValid()) {
                count++;
                it.next();
            }
            return count;
        } finally {
            it.dispose();
        }
    }
}

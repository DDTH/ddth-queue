package com.github.ddth.queue.impl;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.commons.rocksdb.RocksDbUtils;
import com.github.ddth.commons.rocksdb.RocksDbWrapper;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;
import com.github.ddth.queue.utils.QueueUtils;

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
public abstract class RocksDbQueue<ID, DATA> extends AbstractEphemeralSupportQueue<ID, DATA> {

    static {
        RocksDB.loadLibrary();
    }

    private final Logger LOGGER = LoggerFactory.getLogger(RocksDbQueue.class);

    public final static String DEFAULT_STORAGE_DIR = "/tmp/ddth-rocksdb-queue";
    public final static String DEFAULT_CFNAME_QUEUE = "queue";
    public final static String DEFAULT_CFNAME_METADATA = "metadata";
    public final static String DEFAULT_CFNAME_EPHEMERAL = "ephemeral";

    private byte[] lastFetchedId = null;
    private Lock lockPut = new ReentrantLock(), lockTake = new ReentrantLock();

    private String storageDir = DEFAULT_STORAGE_DIR + "/" + System.currentTimeMillis();
    private String cfNameQueue = DEFAULT_CFNAME_QUEUE, cfNameMetadata = DEFAULT_CFNAME_METADATA,
            cfNameEphemeral = DEFAULT_CFNAME_EPHEMERAL;
    private DBOptions dbOptions;
    private ReadOptions readOptions;
    private WriteOptions writeOptions;
    private RocksDbWrapper rocksDbWrapper;
    private WriteBatch batchPutToQueue, batchTake;
    private ColumnFamilyHandle cfQueue, cfMetadata, cfEphemeral;
    private RocksIterator itQueue, itEphemeral;

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
    public RocksDbQueue<ID, DATA> setStorageDir(String storageDir) {
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
    public RocksDbQueue<ID, DATA> setCfNameQueue(String cfNameQueue) {
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
    public RocksDbQueue<ID, DATA> setCfNameMetadata(String cfNameMetadata) {
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
    public RocksDbQueue<ID, DATA> setCfNameEphemeral(String cfNameEphemeral) {
        this.cfNameEphemeral = cfNameEphemeral;
        return this;
    }

    /*----------------------------------------------------------------------*/

    /**
     * Init method.
     * 
     * @return
     * @throws Exception
     */
    public RocksDbQueue<ID, DATA> init() throws Exception {
        File STORAGE_DIR = new File(storageDir);
        LOGGER.info("Storage Directory: " + STORAGE_DIR.getAbsolutePath());
        try {
            FileUtils.forceMkdir(STORAGE_DIR);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        try {
            batchPutToQueue = new WriteBatch();
            batchTake = new WriteBatch();

            dbOptions = RocksDbUtils.buildDbOptions();
            rocksDbWrapper = RocksDbWrapper.openReadWrite(STORAGE_DIR, dbOptions, null, null,
                    new String[] { cfNameEphemeral, cfNameMetadata, cfNameQueue });
            readOptions = rocksDbWrapper.getReadOptions();
            writeOptions = rocksDbWrapper.getWriteOptions();

            cfEphemeral = rocksDbWrapper.getColumnFamilyHandle(cfNameEphemeral);
            cfMetadata = rocksDbWrapper.getColumnFamilyHandle(cfNameMetadata);
            cfQueue = rocksDbWrapper.getColumnFamilyHandle(cfNameQueue);

            itQueue = rocksDbWrapper.getIterator(cfNameQueue);
            itEphemeral = rocksDbWrapper.getIterator(cfNameEphemeral);
            lastFetchedId = loadLastFetchedId();
        } catch (Exception e) {
            destroy();
            throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        }

        super.init();

        return this;
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        try {
            super.destroy();
        } finally {
            try {
                saveLastFetchedId(lastFetchedId);
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }

            RocksDbUtils.closeRocksObjects(batchPutToQueue, batchTake, dbOptions);

            try {
                rocksDbWrapper.close();
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }
    }

    // private final static byte[] keyLastFetchedId =
    // "last-fetched-id".getBytes(QueueUtils.UTF8);
    private final static String keyLastFetchedId = "last-fetched-id";
    private final static byte[] keyLastFetchedIdBytes = keyLastFetchedId.getBytes(QueueUtils.UTF8);

    /**
     * Loads last saved last-fetched-id.
     * 
     * @return
     * @since 0.4.0.1
     */
    private byte[] loadLastFetchedId() {
        // return rocksDbWrapper.get(cfMetadata, readOptions, keyLastFetchedId);
        return rocksDbWrapper.get(cfNameMetadata, readOptions, keyLastFetchedId);
    }

    /**
     * Saves last-fetched-id.
     * 
     * @param lastFetchedId
     * @since 0.4.0.1
     */
    private void saveLastFetchedId(byte[] lastFetchedId) {
        if (lastFetchedId != null) {
            rocksDbWrapper.put(cfNameMetadata, writeOptions, keyLastFetchedId, lastFetchedId);
        }
    }

    protected boolean putToQueue(IQueueMessage<ID, DATA> msg, boolean removeFromEphemeral)
            throws RocksDBException {
        byte[] value = serialize(msg);
        lockPut.lock();
        try {
            byte[] key = QueueUtils.IDGEN.generateId128Hex().toLowerCase()
                    .getBytes(QueueUtils.UTF8);
            try {
                batchPutToQueue.put(cfQueue, key, value);
                if (removeFromEphemeral && !isEphemeralDisabled()) {
                    byte[] _key = msg.getId().toString().getBytes(QueueUtils.UTF8);
                    batchPutToQueue.delete(cfEphemeral, _key);
                }
                rocksDbWrapper.write(writeOptions, batchPutToQueue);
            } finally {
                batchPutToQueue.clear();
            }
            return true;
        } finally {
            lockPut.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean queue(IQueueMessage<ID, DATA> _msg) {
        IQueueMessage<ID, DATA> msg = _msg.clone();
        Date now = new Date();
        msg.setNumRequeues(0).setQueueTimestamp(now).setTimestamp(now);
        try {
            return putToQueue(msg, false);
        } catch (RocksDBException e) {
            throw new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeue(IQueueMessage<ID, DATA> _msg) {
        IQueueMessage<ID, DATA> msg = _msg.clone();
        Date now = new Date();
        msg.incNumRequeues().setQueueTimestamp(now);
        try {
            return putToQueue(msg, true);
        } catch (RocksDBException e) {
            throw new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeueSilent(IQueueMessage<ID, DATA> msg) {
        try {
            return putToQueue(msg.clone(), true);
        } catch (RocksDBException e) {
            throw new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage<ID, DATA> msg) {
        if (!isEphemeralDisabled()) {
            String key = msg.getId().toString();
            rocksDbWrapper.delete(cfNameEphemeral, writeOptions, key);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @throws QueueException.EphemeralIsFull
     *             if the ephemeral storage is full
     */
    @Override
    public IQueueMessage<ID, DATA> take() throws QueueException.EphemeralIsFull {
        if (!isEphemeralDisabled()) {
            int ephemeralMaxSize = getEphemeralMaxSize();
            if (ephemeralMaxSize > 0 && ephemeralSize() >= ephemeralMaxSize) {
                throw new QueueException.EphemeralIsFull(ephemeralMaxSize);
            }
        }
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
            IQueueMessage<ID, DATA> msg = deserialize(value);
            try {
                batchTake.delete(cfQueue, lastFetchedId);
                // batchTake.put(cfMetadata, keyLastFetchedId, lastFetchedId);
                batchTake.put(cfMetadata, keyLastFetchedIdBytes, lastFetchedId);
                if (!isEphemeralDisabled() && msg != null) {
                    byte[] _key = msg.getId().toString().getBytes(QueueUtils.UTF8);
                    batchTake.put(cfEphemeral, _key, value);
                }
                rocksDbWrapper.write(writeOptions, batchTake);
            } catch (RocksDBException e) {
                throw new QueueException(e);
            } finally {
                batchTake.clear();
            }
            itQueue.next();
            return msg;
        } finally {
            lockTake.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<IQueueMessage<ID, DATA>> getOrphanMessages(long thresholdTimestampMs) {
        if (isEphemeralDisabled()) {
            return null;
        }
        synchronized (itEphemeral) {
            Collection<IQueueMessage<ID, DATA>> orphanMessages = new HashSet<>();
            long now = System.currentTimeMillis();
            itEphemeral.seekToFirst();
            while (itEphemeral.isValid()) {
                byte[] value = itEphemeral.value();
                IQueueMessage<ID, DATA> msg = deserialize(value);
                if (msg.getQueueTimestamp().getTime() + thresholdTimestampMs < now) {
                    orphanMessages.add(msg);
                }
                itEphemeral.next();
            }
            return orphanMessages;
        }
    }

//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public boolean moveFromEphemeralToQueueStorage(IQueueMessage<ID, DATA> msg) {
//        try {
//            return putToQueue(msg, true);
//        } catch (RocksDBException e) {
//            throw new QueueException(e);
//        }
//    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        return (int) rocksDbWrapper.getEstimateNumKeys(cfNameQueue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        return (int) rocksDbWrapper.getEstimateNumKeys(cfNameEphemeral);
    }
}

package com.github.ddth.queue.impl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.commons.io.FileUtils;
import org.rocksdb.Env;
import org.rocksdb.Options;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SkipListMemTableConfig;
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
    private RocksIterator rocksDbIt;
    private Options rocksOptions;
    private WriteOptions writeOptions;
    private ReadOptions readOptions;
    private IdGenerator idGen = IdGenerator.getInstance(IdGenerator.getMacAddr());
    private byte[] lastFetchedId = null;
    private Lock lockPut = new ReentrantLock(), lockTake = new ReentrantLock();

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

        rocksOptions = new Options();
        rocksOptions.setCreateIfMissing(true).getEnv().setBackgroundThreads(2, Env.FLUSH_POOL)
                .setBackgroundThreads(4, Env.COMPACTION_POOL);
        rocksOptions.setMaxBackgroundFlushes(2).setMaxBackgroundCompactions(4);
        rocksOptions.setWriteBufferSize(8 * 1024L * 1024L).setMinWriteBufferNumberToMerge(2)
                .setLevelZeroFileNumCompactionTrigger(512).setTargetFileSizeBase(16 * 1024 * 1024);

        /*
         * SkipListMemTable "seems* to have better read performance than the
         * others.
         */
        rocksOptions.setMemTableConfig(new SkipListMemTableConfig());
        // rocksOptions.setMemTableConfig(new HashSkipListMemTableConfig());
        // rocksOptions.setMemTableConfig(new HashLinkedListMemTableConfig());

        /*
         * As of RocksDB v3.10.1, no data lost if process crashes even if
         * sync==false. Data lost only when server/OS crashes. So it's
         * relatively safe to set sync=false for fast write.
         */
        writeOptions = new WriteOptions().setSync(false).setDisableWAL(false);

        readOptions = new ReadOptions().setTailing(true);

        try {
            rocksDb = RocksDB.open(rocksOptions, STORAGE_DIR.getAbsolutePath());
        } catch (RocksDBException e) {
            destroy();
            throw new RuntimeException(e);
        }

        rocksDbIt = rocksDb.newIterator(readOptions);

        return this;
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        if (rocksDbIt != null) {
            try {
                rocksDbIt.dispose();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                rocksDbIt = null;
            }
        }

        if (rocksDb != null) {
            try {
                rocksDb.close();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                rocksDb = null;
            }
        }

        if (rocksOptions != null) {
            try {
                rocksOptions.dispose();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                rocksOptions = null;
            }
        }

        if (readOptions != null) {
            try {
                readOptions.dispose();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                readOptions = null;
            }
        }

        if (writeOptions != null) {
            try {
                writeOptions.dispose();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            } finally {
                writeOptions = null;
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        destroy();
    }

    /**
     * Serializes a queue message to store in Redis.
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

    protected boolean putToQueue(IQueueMessage msg) {
        lockPut.lock();
        try {
            byte[] key = idGen.generateId128Hex().toLowerCase().getBytes();
            byte[] value = serialize(msg);
            try {
                rocksDb.put(writeOptions, key, value);
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
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeue(IQueueMessage _msg) {
        IQueueMessage msg = _msg.clone();
        Date now = new Date();
        msg.qIncNumRequeues().qTimestamp(now);
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeueSilent(IQueueMessage msg) {
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage msg) {
        // EMPTY
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IQueueMessage take() {
        lockTake.lock();
        try {
            if (lastFetchedId == null) {
                rocksDbIt.seekToFirst();
            } else {
                rocksDbIt.seek(lastFetchedId);
            }
            if (!rocksDbIt.isValid()) {
                rocksDbIt.next();
            }
            if (!rocksDbIt.isValid()) {
                return null;
            }
            lastFetchedId = rocksDbIt.key();
            byte[] value = rocksDbIt.value();
            try {
                rocksDb.remove(lastFetchedId);
            } catch (RocksDBException e) {
            }
            rocksDbIt.next();
            return deserialize(value);
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
        throw new UnsupportedOperationException(
                "Method [moveFromEphemeralToQueueStorage] is not supported!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        return -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        return -1;
    }
}

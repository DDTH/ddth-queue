package com.github.ddth.queue.impl.rocksdb;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.WriteOptions;

/**
 * Wrapper around {@link RocksDB} instance.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.5.0
 */
public class RocksDbWrapper implements AutoCloseable {
    static {
        RocksDB.loadLibrary();
    }

    public final static String DEFAULT_COLUMN_FAMILY = new String(RocksDB.DEFAULT_COLUMN_FAMILY,
            RocksDbUtils.UTF8);

    /**
     * Opens a RocksDB with default options in read-only mode.
     * 
     * @param directory
     *            existing RocksDB data directory
     * @return
     * @throws RocksDBException
     */
    public static RocksDbWrapper openReadOnly(File directory) throws RocksDBException {
        RocksDbWrapper rocksDbWrapper = new RocksDbWrapper(directory, true);
        rocksDbWrapper.init();
        return rocksDbWrapper;
    }

    /**
     * Opens a RocksDB with default options in read-only mode.
     * 
     * @param dirPath
     *            existing RocksDB data directory
     * @return
     * @throws RocksDBException
     */
    public static RocksDbWrapper openReadOnly(String dirPath) throws RocksDBException {
        RocksDbWrapper rocksDbWrapper = new RocksDbWrapper(dirPath, true);
        rocksDbWrapper.init();
        return rocksDbWrapper;
    }

    /**
     * Opens a RocksDB with specified options in read-only mode.
     * 
     * @param directory
     *            existing RocksDB data directory
     * @param dbOptions
     * @param readOptions
     * @return
     * @throws RocksDBException
     */
    public static RocksDbWrapper openReadOnly(File directory, DBOptions dbOptions,
            ReadOptions readOptions) throws RocksDBException {
        RocksDbWrapper rocksDbWrapper = new RocksDbWrapper(directory, true);
        rocksDbWrapper.setDbOptions(dbOptions).setReadOptions(readOptions);
        rocksDbWrapper.init();
        return rocksDbWrapper;
    }

    /**
     * Opens a RocksDB with specified options in read-only mode.
     * 
     * @param dirPath
     *            existing RocksDB data directory
     * @param dbOptions
     * @param readOptions
     * @return
     * @throws RocksDBException
     */
    public static RocksDbWrapper openReadOnly(String dirPath, DBOptions dbOptions,
            ReadOptions readOptions) throws RocksDBException {
        RocksDbWrapper rocksDbWrapper = new RocksDbWrapper(dirPath, true);
        rocksDbWrapper.setDbOptions(dbOptions).setReadOptions(readOptions);
        rocksDbWrapper.init();
        return rocksDbWrapper;
    }

    /**
     * Opens a RocksDB with default options in read/write mode.
     * 
     * @param directory
     *            directory to store RocksDB data
     * @param columnFamilies
     *            list of column families to store key/value (the column family
     *            "default" will be automatically added)
     * @return
     * @throws RocksDBException
     */
    public static RocksDbWrapper openReadWrite(File directory, String[] columnFamilies)
            throws RocksDBException {
        RocksDbWrapper rocksDbWrapper = new RocksDbWrapper(directory, false);
        rocksDbWrapper.setColumnFamilies(RocksDbUtils.buildColumnFamilyDescriptors(columnFamilies));
        rocksDbWrapper.init();
        return rocksDbWrapper;
    }

    /**
     * Opens a RocksDB with default options in read/write mode.
     * 
     * @param dirPath
     *            directory to store RocksDB data
     * @param columnFamilies
     *            list of column families to store key/value (the column family
     *            "default" will be automatically added)
     * @return
     * @throws RocksDBException
     */
    public static RocksDbWrapper openReadWrite(String dirPath, String[] columnFamilies)
            throws RocksDBException {
        RocksDbWrapper rocksDbWrapper = new RocksDbWrapper(dirPath, false);
        rocksDbWrapper.setColumnFamilies(RocksDbUtils.buildColumnFamilyDescriptors(columnFamilies));
        rocksDbWrapper.init();
        return rocksDbWrapper;
    }

    /**
     * Opens a RocksDB with specified options in read/write mode.
     * 
     * @param directory
     *            directory to store RocksDB data
     * @param dbOptions
     * @param readOptions
     * @param writeOptions
     * @param columnFamilies
     *            list of column families to store key/value (the column family
     *            "default" will be automatically added)
     * @return
     * @throws RocksDBException
     */
    public static RocksDbWrapper openReadWrite(File directory, DBOptions dbOptions,
            ReadOptions readOptions, WriteOptions writeOptions, String[] columnFamilies)
            throws RocksDBException {
        RocksDbWrapper rocksDbWrapper = new RocksDbWrapper(directory, false);
        rocksDbWrapper.setDbOptions(dbOptions).setReadOptions(readOptions)
                .setWriteOptions(writeOptions);
        rocksDbWrapper.setColumnFamilies(RocksDbUtils.buildColumnFamilyDescriptors(columnFamilies));
        rocksDbWrapper.init();
        return rocksDbWrapper;
    }

    /**
     * Opens a RocksDB with specified options in read/write mode.
     * 
     * @param dirPath
     *            directory to store RocksDB data
     * @param dbOptions
     * @param readOptions
     * @param writeOptions
     * @param columnFamilies
     *            list of column families to store key/value (the column family
     *            "default" will be automatically added)
     * @return
     * @throws RocksDBException
     */
    public static RocksDbWrapper openReadWrite(String dirPath, DBOptions dbOptions,
            ReadOptions readOptions, WriteOptions writeOptions, String[] columnFamilies)
            throws RocksDBException {
        RocksDbWrapper rocksDbWrapper = new RocksDbWrapper(dirPath, false);
        rocksDbWrapper.setDbOptions(dbOptions).setReadOptions(readOptions)
                .setWriteOptions(writeOptions);
        rocksDbWrapper.setColumnFamilies(RocksDbUtils.buildColumnFamilyDescriptors(columnFamilies));
        rocksDbWrapper.init();
        return rocksDbWrapper;
    }
    /*----------------------------------------------------------------------*/

    private File directory;
    private final boolean readOnly;
    private RocksDB rocksDb;

    private DBOptions dbOptions;
    private boolean myOwnDbOptions = true;

    private WriteOptions writeOptions;
    private boolean myOwnWriteOptions = true;

    private ReadOptions readOptions;
    private boolean myOwnReadOptions = true;

    private Set<ColumnFamilyDescriptor> columnFamilies = new HashSet<>();
    private Map<String, ColumnFamilyHandle> columnFamilyHandles = new HashMap<>();
    private Map<String, RocksIterator> iterators = new HashMap<>();

    public RocksDbWrapper(String dirPath, boolean readOnly) {
        this(new File(dirPath), readOnly);
    }

    public RocksDbWrapper(File directory, boolean readOnly) {
        this.directory = directory;
        this.readOnly = readOnly;
    }

    public RocksDbWrapper setColumnFamilies(Collection<ColumnFamilyDescriptor> columnFamilies) {
        if (columnFamilies != null) {
            this.columnFamilies.addAll(columnFamilies);
        }
        return this;
    }

    public Collection<ColumnFamilyDescriptor> getColumnFamilies() {
        return Collections.unmodifiableSet(columnFamilies);
    }

    public Collection<String> getColumnFamilyNames() {
        Collection<String> result = new HashSet<>();
        columnFamilies.iterator().forEachRemaining(x -> {
            result.add(new String(x.columnFamilyName(), RocksDbUtils.UTF8));
        });
        return result;
    }

    public RocksDbWrapper setDbOptions(DBOptions dbOptions) {
        if (this.dbOptions != null) {
            RocksDbUtils.closeRocksObjects(this.dbOptions);
        }
        this.dbOptions = dbOptions;
        myOwnDbOptions = false;
        return this;
    }

    public DBOptions getDbOptions() {
        return this.dbOptions;
    }

    public RocksDbWrapper setReadOptions(ReadOptions readOptions) {
        if (this.readOptions != null) {
            RocksDbUtils.closeRocksObjects(this.readOptions);
        }
        this.readOptions = readOptions;
        myOwnReadOptions = false;
        return this;
    }

    public ReadOptions getReadOptions() {
        return this.readOptions;
    }

    public RocksDbWrapper setWriteOptions(WriteOptions writeOptions) {
        if (this.writeOptions != null) {
            RocksDbUtils.closeRocksObjects(this.writeOptions);
        }
        this.writeOptions = writeOptions;
        myOwnWriteOptions = false;
        return this;
    }

    public WriteOptions getWriteOptions() {
        return this.writeOptions;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception {
        destroy();
    }

    public void destroy() {
        for (Entry<String, RocksIterator> entry : iterators.entrySet()) {
            RocksDbUtils.closeRocksObjects(entry.getValue());
        }

        if (myOwnReadOptions) {
            RocksDbUtils.closeRocksObjects(readOptions);
        }

        if (myOwnWriteOptions) {
            RocksDbUtils.closeRocksObjects(writeOptions);
        }

        if (myOwnDbOptions) {
            RocksDbUtils.closeRocksObjects(dbOptions);
        }

        // for (ColumnFamilyDescriptor cfd : columnFamilies) {
        // RocksDbUtils.closeRocksObjects(cfd);
        // }

        for (Entry<String, ColumnFamilyHandle> entry : columnFamilyHandles.entrySet()) {
            RocksDbUtils.closeRocksObjects(entry.getValue());
        }

        RocksDbUtils.closeRocksObjects(rocksDb);

    }

    public RocksDbWrapper init() throws RocksDBException {
        if (!directory.isDirectory() || !directory.exists()) {
            throw new IllegalArgumentException(directory + " does not exist, or not a directory!");
        }

        prepareColumnFamilyDescriptors();

        if (dbOptions == null) {
            dbOptions = RocksDbUtils.buildDbOptions();
            myOwnDbOptions = true;
        } else {
            myOwnDbOptions = false;
        }

        String path = directory.getAbsolutePath();
        List<ColumnFamilyDescriptor> cfdList = new ArrayList<>(columnFamilies);
        List<ColumnFamilyHandle> cfhList = new ArrayList<>();
        if (readOnly) {
            if (readOptions == null) {
                readOptions = RocksDbUtils.buildReadOptions(true);
                myOwnReadOptions = true;
            } else {
                myOwnReadOptions = false;
            }
            rocksDb = RocksDB.openReadOnly(dbOptions, path, cfdList, cfhList);
        } else {
            if (writeOptions == null) {
                /*
                 * As of RocksDB v3.10.1, no data lost if process crashes even
                 * if sync==false. Data lost only when server/OS crashes. So
                 * it's relatively safe to set sync=false for fast write.
                 */
                writeOptions = RocksDbUtils.buildWriteOptions(false, false);
                myOwnWriteOptions = true;
            } else {
                myOwnWriteOptions = false;
            }
            if (readOptions == null) {
                readOptions = RocksDbUtils.buildReadOptions(true);
                myOwnReadOptions = true;
            } else {
                myOwnReadOptions = false;
            }
            rocksDb = RocksDB.open(dbOptions, path, cfdList, cfhList);
        }

        for (int i = 0, n = cfdList.size(); i < n; i++) {
            byte[] cfName = cfdList.get(i).columnFamilyName();
            ColumnFamilyHandle cfh = cfhList.get(i);
            columnFamilyHandles.put(new String(cfName, RocksDbUtils.UTF8), cfh);
        }

        return this;
    }

    private void prepareColumnFamilyDescriptors() throws RocksDBException {
        if (columnFamilies == null || columnFamilies.size() == 0) {
            if (columnFamilies == null) {
                columnFamilies = new HashSet<>();
            }
            String[] cfList = RocksDbUtils.getColumnFamilyList(directory.getAbsolutePath());
            for (String cf : cfList) {
                columnFamilies.add(RocksDbUtils.buildColumnFamilyDescriptor(cf));
            }
        }

        boolean hasDefaultCf = false;
        for (ColumnFamilyDescriptor cfd : columnFamilies) {
            if (Arrays.equals(cfd.columnFamilyName(), RocksDB.DEFAULT_COLUMN_FAMILY)) {
                hasDefaultCf = true;
                break;
            }
        }
        if (!hasDefaultCf) {
            columnFamilies.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY));
        }
    }

    /*----------------------------------------------------------------------*/
    public ColumnFamilyHandle getColumnFamilyHandle(String cfName) {
        return columnFamilyHandles.get(cfName);
    }

    /**
     * See {@link RocksDB#compactRange()}.
     * 
     * @throws RocksDbException
     */
    public void compactRange() throws RocksDbException {
        try {
            rocksDb.compactRange();
        } catch (RocksDBException e) {
            throw new RocksDbException(e);
        }
    }

    /**
     * See {@link RocksDB#getProperty(ColumnFamilyHandle, String)}.
     * 
     * @param cfName
     * @param name
     * @return
     * @throws RocksDbException
     */
    public String getProperty(String cfName, String name) throws RocksDbException {
        ColumnFamilyHandle cfh = getColumnFamilyHandle(cfName);
        if (cfh == null) {
            return null;
        }
        try {
            return rocksDb.getProperty(cfh, name);
        } catch (RocksDBException e) {
            throw new RocksDbException(e);
        }
    }

    /**
     * Gets estimated number of keys for a column family.
     * 
     * @param cfName
     * @return
     * @throws RocksDbException
     */
    public long getEstimateNumKeys(String cfName) throws RocksDbException {
        String prop = getProperty(cfName, "rocksdb.estimate-num-keys");
        return prop != null ? Long.parseLong(prop) : 0;
    }

    /**
     * Obtains an iterator for a column family.
     * 
     * <p>
     * Iterators will be automatically closed by this wrapper.
     * </p>
     * 
     * @param cfName
     * @return
     * @throws RocksDbException
     */
    public RocksIterator getIterator(String cfName) throws RocksDbException {
        synchronized (iterators) {
            RocksIterator it = iterators.get(cfName);
            if (it == null) {
                ColumnFamilyHandle cfh = getColumnFamilyHandle(cfName);
                if (cfh == null) {
                    throw new RocksDbException.ColumnFamilyNotExists(cfName);
                }
                it = rocksDb.newIterator(cfh, readOptions);
                iterators.put(cfName, it);
            }
            return it;
        }

    }

    /*----------------------------------------------------------------------*/
    /**
     * Deletes a key from the default family.
     * 
     * @param key
     * @throws RocksDbException
     */
    public void delete(String key) throws RocksDbException {
        delete(DEFAULT_COLUMN_FAMILY, key);
    }

    /**
     * Deletes a key from a column family.
     * 
     * @param cfName
     * @param key
     * @throws RocksDbException
     */
    public void delete(String cfName, String key) throws RocksDbException {
        delete(cfName, writeOptions, key);
    }

    /**
     * Deletes a key from a column family, specifying write options.
     * 
     * @param cfName
     * @param writeOptions
     * @param key
     * @throws RocksDbException
     */
    public void delete(String cfName, WriteOptions writeOptions, String key)
            throws RocksDbException {
        if (cfName == null) {
            cfName = DEFAULT_COLUMN_FAMILY;
        }
        ColumnFamilyHandle cfh = getColumnFamilyHandle(cfName);
        if (cfh == null) {
            throw new RocksDbException.ColumnFamilyNotExists(cfName);
        }
        delete(cfh, writeOptions != null ? writeOptions : this.writeOptions,
                key.getBytes(RocksDbUtils.UTF8));
    }

    /**
     * Deletes a key.
     * 
     * @param cfh
     * @param writeOptions
     * @param key
     * @throws RocksDbException
     */
    public void delete(ColumnFamilyHandle cfh, WriteOptions writeOptions, byte[] key)
            throws RocksDbException {
        if (readOnly) {
            throw new RocksDbException.ReadOnlyException();
        }
        try {
            rocksDb.delete(cfh, writeOptions, key);
        } catch (RocksDBException e) {
            throw new RocksDbException(e);
        }
    }

    /*----------------------------------------------------------------------*/
    /**
     * Puts a key/value to the default column family.
     * 
     * @param key
     * @param value
     * @throws RocksDbException
     */
    public void put(String key, String value) throws RocksDbException {
        put(DEFAULT_COLUMN_FAMILY, key, value);
    }

    /**
     * Puts a key/value to the default column family.
     * 
     * @param key
     * @param value
     * @throws RocksDbException
     */
    public void put(String key, byte[] value) throws RocksDbException {
        put(DEFAULT_COLUMN_FAMILY, key, value);
    }

    /**
     * Puts a key/value to a column family.
     * 
     * @param cfName
     * @param key
     * @param value
     * @throws RocksDbException
     */
    public void put(String cfName, String key, String value) throws RocksDbException {
        put(cfName, key, value != null ? value.getBytes(RocksDbUtils.UTF8) : null);
    }

    /**
     * Puts a key/value to a column family.
     * 
     * @param cfName
     * @param key
     * @param value
     * @throws RocksDbException
     */
    public void put(String cfName, String key, byte[] value) throws RocksDbException {
        put(cfName, writeOptions, key, value);
    }

    /**
     * Puts a key/value to a column family, specifying write options.
     * 
     * @param cfName
     * @param writeOptions
     * @param key
     * @param value
     * @throws RocksDbException
     */
    public void put(String cfName, WriteOptions writeOptions, String key, byte[] value)
            throws RocksDbException {
        if (cfName == null) {
            cfName = DEFAULT_COLUMN_FAMILY;
        }
        ColumnFamilyHandle cfh = columnFamilyHandles.get(cfName);
        if (cfh == null) {
            throw new RocksDbException.ColumnFamilyNotExists(cfName);
        }
        put(cfh, writeOptions != null ? writeOptions : this.writeOptions,
                key.getBytes(RocksDbUtils.UTF8), value);
    }

    /**
     * Puts a key/value.
     * 
     * @param cfh
     * @param writeOptions
     * @param key
     * @param value
     * @throws RocksDbException
     */
    public void put(ColumnFamilyHandle cfh, WriteOptions writeOptions, byte[] key, byte[] value)
            throws RocksDbException {
        if (readOnly) {
            throw new RocksDbException.ReadOnlyException();
        }
        if (value == null) {
            delete(cfh, writeOptions, key);
        }
        try {
            rocksDb.put(cfh, writeOptions, key, value);
        } catch (RocksDBException e) {
            throw new RocksDbException(e);
        }
    }

    /*----------------------------------------------------------------------*/
    /**
     * Gets a value from the default column family.
     * 
     * @param key
     * @return
     * @throws RocksDbException
     */
    public byte[] get(String key) throws RocksDbException {
        return get(DEFAULT_COLUMN_FAMILY, key);
    }

    /**
     * Gets a value from a column family.
     * 
     * @param cfName
     * @param key
     * @return
     * @throws RocksDbException
     */
    public byte[] get(String cfName, String key) throws RocksDbException {
        return get(cfName, readOptions, key);
    }

    /**
     * Gets a value from a column family, specifying read options.
     * 
     * @param cfName
     * @param readOptions
     * @param key
     * @return
     * @throws RocksDbException
     */
    public byte[] get(String cfName, ReadOptions readOptions, String key) throws RocksDbException {
        if (cfName == null) {
            cfName = DEFAULT_COLUMN_FAMILY;
        }
        ColumnFamilyHandle cfh = columnFamilyHandles.get(cfName);
        if (cfh == null) {
            throw new RocksDbException.ColumnFamilyNotExists(cfName);
        }
        return get(cfh, readOptions != null ? readOptions : this.readOptions,
                key.getBytes(RocksDbUtils.UTF8));
    }

    /**
     * Gets a value.
     * 
     * @param cfh
     * @param readOptions
     * @param key
     * @return
     * @throws RocksDbException
     */
    public byte[] get(ColumnFamilyHandle cfh, ReadOptions readOptions, byte[] key)
            throws RocksDbException {
        try {
            return rocksDb.get(cfh, readOptions, key);
        } catch (RocksDBException e) {
            throw new RocksDbException(e);
        }
    }

    /*----------------------------------------------------------------------*/
    /**
     * See {@link RocksDB#write(WriteOptions, WriteBatch)}.
     * 
     * @param batch
     * @throws RocksDbException
     */
    public void write(WriteBatch batch) throws RocksDbException {
        write(writeOptions, batch);
    }

    /**
     * See {@link RocksDB#write(WriteOptions, WriteBatch)}.
     * 
     * @param writeOptions
     * @param batch
     * @throws RocksDbException
     */
    public void write(WriteOptions writeOptions, WriteBatch batch) throws RocksDbException {
        try {
            rocksDb.write(writeOptions != null ? writeOptions : this.writeOptions, batch);
        } catch (RocksDBException e) {
            throw new RocksDbException(e);
        }
    }

    /**
     * See {@link RocksDB#write(WriteOptions, WriteBatchWithIndex)}.
     * 
     * @param batch
     * @throws RocksDbException
     */
    public void write(WriteBatchWithIndex batch) throws RocksDbException {
        write(writeOptions, batch);
    }

    /**
     * See {@link RocksDB#write(WriteOptions, WriteBatchWithIndex)}.
     * 
     * @param writeOptions
     * @param batch
     * @throws RocksDbException
     */
    public void write(WriteOptions writeOptions, WriteBatchWithIndex batch)
            throws RocksDbException {
        try {
            rocksDb.write(writeOptions != null ? writeOptions : this.writeOptions, batch);
        } catch (RocksDBException e) {
            throw new RocksDbException(e);
        }
    }
}

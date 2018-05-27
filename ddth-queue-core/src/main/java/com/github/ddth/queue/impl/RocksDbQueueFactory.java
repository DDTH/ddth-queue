package com.github.ddth.queue.impl;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.queue.QueueSpec;

/**
 * Factory to create {@link RocksDb} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public abstract class RocksDbQueueFactory<T extends RocksDbQueue<ID, DATA>, ID, DATA>
        extends AbstractQueueFactory<T, ID, DATA> {

    public final static String SPEC_FIELD_STORAGE_DIR = "storage_dir";
    public final static String SPEC_FIELD_CF_EPHEMERAL = "cf_ephemeral";
    public final static String SPEC_FIELD_CF_QUEUE = "cf_queue";
    public final static String SPEC_FIELD_CF_METADATA = "cf_metadata";

    private String defaultStorageDir;
    private String defaultCfNameQueue = RocksDbQueue.DEFAULT_CFNAME_QUEUE,
            defaultCfNameMetaData = RocksDbQueue.DEFAULT_CFNAME_METADATA,
            defaultCfNameEphemeral = RocksDbQueue.DEFAULT_CFNAME_EPHEMERAL;

    public String getDefaultStorageDir() {
        return defaultStorageDir;
    }

    public RocksDbQueueFactory<T, ID, DATA> setDefaultStorageDir(String defaultStorageDir) {
        this.defaultStorageDir = defaultStorageDir;
        return this;
    }

    public String getDefaultCfNameQueue() {
        return defaultCfNameQueue;
    }

    public RocksDbQueueFactory<T, ID, DATA> setDefaultCfNameQueue(String defaultCfNameQueue) {
        this.defaultCfNameQueue = defaultCfNameQueue;
        return this;
    }

    public String getDefaultCfNameMetaData() {
        return defaultCfNameMetaData;
    }

    public RocksDbQueueFactory<T, ID, DATA> setDefaultCfNameMetaData(String defaultCfNameMetaData) {
        this.defaultCfNameMetaData = defaultCfNameMetaData;
        return this;
    }

    public String getDefaultCfNameEphemeral() {
        return defaultCfNameEphemeral;
    }

    public RocksDbQueueFactory<T, ID, DATA> setDefaultCfNameEphemeral(
            String defaultCfNameEphemeral) {
        this.defaultCfNameEphemeral = defaultCfNameEphemeral;
        return this;
    }

    /**
     * {@inheritDoc}
     * 
     * @throws Exception
     */
    @Override
    protected void initQueue(T queue, QueueSpec spec) throws Exception {
        queue.setEphemeralDisabled(getDefaultEphemeralDisabled())
                .setEphemeralMaxSize(getDefaultEphemeralMaxSize());
        Boolean ephemeralDisabled = spec.getField(QueueSpec.FIELD_EPHEMERAL_DISABLED,
                Boolean.class);
        if (ephemeralDisabled != null) {
            queue.setEphemeralDisabled(ephemeralDisabled.booleanValue());
        }
        Integer maxEphemeralSize = spec.getField(QueueSpec.FIELD_EPHEMERAL_MAX_SIZE, Integer.class);
        if (maxEphemeralSize != null) {
            queue.setEphemeralMaxSize(maxEphemeralSize.intValue());
        }

        String storageDir = spec.getField(SPEC_FIELD_STORAGE_DIR);
        storageDir = StringUtils.isBlank(storageDir) ? defaultStorageDir : storageDir;
        if (!StringUtils.isBlank(storageDir)) {
            queue.setStorageDir(storageDir);
        }

        queue.setCfNameEphemeral(defaultCfNameEphemeral).setCfNameMetadata(defaultCfNameMetaData)
                .setCfNameQueue(defaultCfNameEphemeral);
        String cfNameEphemeral = spec.getField(SPEC_FIELD_CF_EPHEMERAL);
        if (!StringUtils.isBlank(cfNameEphemeral)) {
            queue.setCfNameEphemeral(cfNameEphemeral);
        }
        String cfNameMetadata = spec.getField(SPEC_FIELD_CF_METADATA);
        if (!StringUtils.isBlank(cfNameMetadata)) {
            queue.setCfNameMetadata(cfNameMetadata);
        }
        String cfNameQueue = spec.getField(SPEC_FIELD_CF_QUEUE);
        if (!StringUtils.isBlank(cfNameQueue)) {
            queue.setCfNameQueue(cfNameQueue);
        }

        super.initQueue(queue, spec);
    }

}

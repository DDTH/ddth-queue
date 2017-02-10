package com.github.ddth.queue.impl;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.queue.QueueSpec;

/**
 * Factory to create {@link RocksDb} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public abstract class RocksDbQueueFactory<T extends RocksDbQueue> extends AbstractQueueFactory<T> {

    public final static String SPEC_FIELD_STORAGE_DIR = "storage_dir";
    public final static String SPEC_FIELD_CF_EPHEMERAL = "cf_ephemeral";
    public final static String SPEC_FIELD_CF_QUEUE = "cf_queue";
    public final static String SPEC_FIELD_CF_METADATA = "cf_metadata";

    private String defaultStorageDir = "/tmp/ddth-rocksdb-queue";

    public String getDefaultStorageDir() {
        return defaultStorageDir;
    }

    public RocksDbQueueFactory<T> setDefaultStorageDir(String defaultStorageDir) {
        this.defaultStorageDir = defaultStorageDir;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void initQueue(T queue, QueueSpec spec) {
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
        if (!StringUtils.isEmpty(storageDir)) {
            queue.setStorageDir(storageDir);
        } else {
            throw new IllegalArgumentException(
                    "Empty or Invalid value for parameter [" + SPEC_FIELD_STORAGE_DIR + "]!");
        }

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

        queue.init();
    }

}

package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.impl.RocksDbQueue;

/**
 * Base class for universal RocksDB queue implementations.
 *
 * @param <T>
 * @author Thanh Nguyen
 * @since 0.4.0
 */
public abstract class BaseUniversalRocksDbQueue<T extends BaseUniversalQueueMessage<ID>, ID>
        extends RocksDbQueue<ID, byte[]> {
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T take() {
        return (T) super.take();
    }
}

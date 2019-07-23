package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.impl.MongodbQueue;

/**
 * Base class for universal MongoDB queue implementations.
 *
 * @param <T>
 * @author Thanh Nguyen
 * @since 0.7.1
 */
public abstract class BaseUniversalMongodbQueue<T extends BaseUniversalQueueMessage<ID>, ID>
        extends MongodbQueue<ID, byte[]> {
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T take() {
        return (T) super.take();
    }
}

package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.impl.RedisQueue;

/**
 * Base class for universal Redis queue implementations.
 * 
 * @author Thanh Nguyen
 * 
 * @param <T>
 * @since 0.3.3
 */
public abstract class BaseUniversalRedisQueue<T extends BaseUniversalQueueMessage<ID>, ID>
        extends RedisQueue<ID, byte[]> {

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T take() {
        return (T) super.take();
    }
}

package com.github.ddth.queue.impl.universal.base;

import com.github.ddth.queue.impl.JdbcQueue;

/**
 * Base class for universal JDBC queue implementations.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * 
 * @param <T>
 * @since 0.6.0
 */
public abstract class BaseUniversalJdbcQueue<T extends BaseUniversalQueueMessage<ID>, ID>
        extends JdbcQueue<ID, byte[]> {
    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T take() {
        return (T) super.take();
    }
}

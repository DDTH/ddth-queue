package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.impl.ActiveMqQueue;

/**
 * Base class for universal ActiveMQ queue implementations.
 *
 * @param <T>
 * @author Thanh Nguyen
 * @since 0.6.1
 */
public abstract class BaseUniversalActiveMqQueue<T extends BaseUniversalQueueMessage<ID>, ID>
        extends ActiveMqQueue<ID, byte[]> {

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T take() {
        return (T) super.take();
    }
}

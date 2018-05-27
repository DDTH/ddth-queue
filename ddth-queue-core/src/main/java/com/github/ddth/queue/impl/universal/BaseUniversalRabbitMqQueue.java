package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.impl.RabbitMqQueue;

/**
 * Base class for universal RabbitMQ queue implementations.
 *
 * @param <T>
 * @author Thanh Nguyen
 * @since 0.6.1
 */
public abstract class BaseUniversalRabbitMqQueue<T extends BaseUniversalQueueMessage<ID>, ID>
        extends RabbitMqQueue<ID, byte[]> {

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T take() {
        return (T) super.take();
    }
}

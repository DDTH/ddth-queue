package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.impl.KafkaQueue;

/**
 * Base class for universal Kafka queue implementations.
 * 
 * @author Thanh Nguyen
 * 
 * @param <T>
 * @since 0.3.3
 */
public abstract class BaseUniversalKafkaQueue<T extends BaseUniversalQueueMessage<ID>, ID>
        extends KafkaQueue<ID, byte[]> {

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T take() {
        return (T) super.take();
    }
}

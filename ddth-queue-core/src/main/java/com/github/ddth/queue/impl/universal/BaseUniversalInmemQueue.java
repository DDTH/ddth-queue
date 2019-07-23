package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.impl.InmemQueue;

/**
 * Base class for universal in-memory queue implementations.
 *
 * @param <T>
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class BaseUniversalInmemQueue<T extends BaseUniversalQueueMessage<ID>, ID> extends InmemQueue<ID, byte[]> {
    public BaseUniversalInmemQueue() {
    }

    public BaseUniversalInmemQueue(int boundary) {
        super(boundary);
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T take() {
        return (T) super.take();
    }
}

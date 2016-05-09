package com.github.ddth.queue.impl;

/**
 * Base class for universal in-memory queue implementations.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * 
 * @param <T>
 * @since 0.4.0
 */
public class BaseUniversalInmemQueue<T extends BaseUniversalQueueMessage> extends InmemQueue {

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

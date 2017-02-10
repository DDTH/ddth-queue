package com.github.ddth.queue.impl.base;

import com.github.ddth.queue.impl.DisruptorQueue;

/**
 * Base class for universal LMAX Disruptor queue implementations.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * 
 * @param <T>
 * @since 0.4.0
 */
public class BaseUniversalDisruptorQueue<T extends BaseUniversalQueueMessage>
        extends DisruptorQueue {

    public BaseUniversalDisruptorQueue() {
    }

    public BaseUniversalDisruptorQueue(int ringSize) {
        super(ringSize);
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

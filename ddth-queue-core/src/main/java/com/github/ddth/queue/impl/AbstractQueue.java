package com.github.ddth.queue.impl;

import java.io.Closeable;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;

/**
 * Abstract queue implementation.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.5.0
 */
public abstract class AbstractQueue<ID, DATA>
        implements IQueue<ID, DATA>, Closeable, AutoCloseable {

    private String queueName;

    /**
     * Get queue's name.
     * 
     * @return
     * @since 0.5.2
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * Set queue's name.
     * 
     * @param queueName
     * @return
     * @since 0.5.2
     */
    public AbstractQueue<ID, DATA> setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    /**
     * Initializing method.
     * 
     * @return
     * @throws Exception
     */
    public AbstractQueue<ID, DATA> init() throws Exception {
        return this;
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        // EMPTY
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        destroy();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.0
     */
    @Override
    public IQueueMessage<ID, DATA> createMessage() {
        return new GenericQueueMessage<>();
    }
}

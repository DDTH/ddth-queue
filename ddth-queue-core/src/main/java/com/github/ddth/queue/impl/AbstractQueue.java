package com.github.ddth.queue.impl;

import java.io.Closeable;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.IQueueObserver;

/**
 * Abstract queue implementation.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.5.0
 */
public abstract class AbstractQueue<ID, DATA>
        implements IQueue<ID, DATA>, Closeable, AutoCloseable {

    private String queueName;
    private IQueueObserver<ID, DATA> observer;

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
     * Get queue's event observers
     * 
     * @return
     */
    public IQueueObserver<ID, DATA> getObserver() {
        return observer;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public AbstractQueue<ID, DATA> setObserver(IQueueObserver<ID, DATA> observer) {
        this.observer = observer;
        return this;
    }

    /**
     * Initializing method.
     * 
     * @return
     * @throws Exception
     */
    public AbstractQueue<ID, DATA> init() throws Exception {
        if (observer != null) {
            observer.preInit(this);
        }
        try {
            return this;
        } finally {
            if (observer != null) {
                observer.postInit(this);
            }
        }
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        if (observer != null) {
            observer.preDestroy(this);
        }
        if (observer != null) {
            observer.postDestroy(this);
        }
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

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.0
     */
    @Override
    public IQueueMessage<ID, DATA> createMessage(DATA content) {
        return new GenericQueueMessage<ID, DATA>().qData(content);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.0
     */
    @Override
    public IQueueMessage<ID, DATA> createMessage(ID id, DATA content) {
        return new GenericQueueMessage<ID, DATA>().qId(id).qData(content);
    }

}

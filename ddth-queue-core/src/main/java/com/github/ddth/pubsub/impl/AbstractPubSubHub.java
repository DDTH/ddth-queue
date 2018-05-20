package com.github.ddth.pubsub.impl;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.queue.IMessage;
import com.github.ddth.queue.IMessageFactory;

/**
 * Abstract implementation of {@link IPubSubHub}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public abstract class AbstractPubSubHub<ID, DATA> implements IPubSubHub<ID, DATA>, AutoCloseable {

    private IMessageFactory<ID, DATA> messageFactory;

    /**
     * Getter for {@link #messageFactory}.
     * 
     * @return
     */
    public IMessageFactory<ID, DATA> getMessageFactory() {
        return messageFactory;
    }

    /**
     * Setter for {@link #messageFactory}.
     * 
     * @param messageFactory
     * @return
     */
    public AbstractPubSubHub<ID, DATA> setMessageFactory(IMessageFactory<ID, DATA> messageFactory) {
        this.messageFactory = messageFactory;
        return this;
    }

    /**
     * Initializing method.
     * 
     * @return
     */
    public AbstractPubSubHub<ID, DATA> init() {
        return this;
    }

    /**
     * Clean-up method.
     */
    public void destroy() {
        // empty
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
     */
    @Override
    public IMessage<ID, DATA> createMessage() {
        return messageFactory.createMessage();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IMessage<ID, DATA> createMessage(DATA data) {
        return messageFactory.createMessage(data);
    }
}

package com.github.ddth.pubsub.impl;

import com.github.ddth.commons.serialization.FstSerDeser;
import com.github.ddth.commons.serialization.ISerDeser;
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
    private ISerDeser serDeser;

    /**
     * Factory to create messages.
     *
     * @return
     */
    public IMessageFactory<ID, DATA> getMessageFactory() {
        return messageFactory;
    }

    /**
     * Factory to create messages.
     *
     * @param messageFactory
     * @return
     */
    public AbstractPubSubHub<ID, DATA> setMessageFactory(IMessageFactory<ID, DATA> messageFactory) {
        this.messageFactory = messageFactory;
        return this;
    }

    /**
     * Message serializer/deserializer.
     *
     * @return
     * @since 1.0.0
     */
    public ISerDeser getSerDeser() {
        return serDeser;
    }

    /**
     * Message serializer/deserializer.
     *
     * @param serDeser
     * @return
     * @since 1.0.0
     */
    public AbstractPubSubHub<ID, DATA> setSerDeser(ISerDeser serDeser) {
        this.serDeser = serDeser;
        return this;
    }

    /**
     * Initializing method.
     *
     * @return
     */
    public AbstractPubSubHub<ID, DATA> init() {
        if (serDeser == null) {
            serDeser = new FstSerDeser();
        }
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

    /**
     * {@inheritDoc}
     */
    @Override
    public IMessage<ID, DATA> createMessage(ID id, DATA data) {
        return messageFactory.createMessage(id, data);
    }

    /**
     * Serialize a queue message to store in Redis.
     *
     * @param msg
     * @return
     */
    protected byte[] serialize(IMessage<ID, DATA> msg) {
        return msg != null ? serDeser.toBytes(msg) : null;
    }

    /**
     * Deserialize a message.
     *
     * @param msgData
     * @return
     */
    @SuppressWarnings("unchecked")
    protected IMessage<ID, DATA> deserialize(byte[] msgData) {
        return deserialize(msgData, IMessage.class);
    }

    /**
     * Deserialize a message.
     *
     * @param msgData
     * @return
     */
    protected <T extends IMessage<ID, DATA>> T deserialize(byte[] msgData, Class<T> clazz) {
        return msgData != null ? serDeser.fromBytes(msgData, clazz) : null;
    }
}

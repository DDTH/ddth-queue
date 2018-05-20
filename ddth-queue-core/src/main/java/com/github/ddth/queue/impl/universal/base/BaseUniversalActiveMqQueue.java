package com.github.ddth.queue.impl.universal.base;

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

    // /**
    // * {@inheritDoc}
    // */
    // @Override
    // protected byte[] serialize(IQueueMessage<ID, byte[]> _msg) throws
    // QueueException {
    // if (_msg == null) {
    // return null;
    // }
    // if (!(_msg instanceof BaseUniversalQueueMessage)) {
    // throw new IllegalArgumentException(
    // "This method requires an argument of type [" +
    // BaseUniversalQueueMessage.class
    // .getName() + "]!");
    // }
    //
    // BaseUniversalQueueMessage<ID> msg = (BaseUniversalQueueMessage<ID>) _msg;
    // try {
    // return msg.toBytes();
    // } catch (Exception e) {
    // throw new QueueException.CannotSerializeQueueMessage(e);
    // }
    // }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T take() {
        return (T) super.take();
    }
}

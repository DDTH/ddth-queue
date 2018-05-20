package com.github.ddth.queue.impl.universal.base;

import com.github.ddth.queue.impl.RedisQueue;

/**
 * Base class for universal Redis queue implementations.
 * 
 * @author Thanh Nguyen
 * 
 * @param <T>
 * @since 0.3.3
 */
public abstract class BaseUniversalRedisQueue<T extends BaseUniversalQueueMessage<ID>, ID>
        extends RedisQueue<ID, byte[]> {

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
    // throw new IllegalArgumentException("This method requires an argument of
    // type ["
    // + BaseUniversalQueueMessage.class.getName() + "]!");
    // }
    //
    // BaseUniversalQueueMessage<ID> msg = (BaseUniversalQueueMessage<ID>) _msg;
    // try {
    // return msg.toBytes();
    // } catch (Exception e) {
    // throw new QueueException.CannotSerializeQueueMessage(e);
    // O}
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

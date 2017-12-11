package com.github.ddth.queue.impl.universal.base;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.RocksDbQueue;
import com.github.ddth.queue.utils.QueueException;

/**
 * Base class for universal RocksDB queue implementations.
 * 
 * @author Thanh Nguyen
 * 
 * @param <T>
 * @since 0.4.0
 */
public abstract class BaseUniversalRocksDbQueue<T extends BaseUniversalQueueMessage<ID>, ID>
        extends RocksDbQueue<ID, byte[]> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected byte[] serialize(IQueueMessage<ID, byte[]> _msg) throws QueueException {
        if (_msg == null) {
            return null;
        }
        if (!(_msg instanceof BaseUniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + BaseUniversalQueueMessage.class.getName() + "]!");
        }

        BaseUniversalQueueMessage<ID> msg = (BaseUniversalQueueMessage<ID>) _msg;
        try {
            return msg.toBytes();
        } catch (Exception e) {
            throw new QueueException.CannotSerializeQueueMessage(e);
        }
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

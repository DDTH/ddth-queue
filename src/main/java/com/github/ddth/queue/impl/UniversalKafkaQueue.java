package com.github.ddth.queue.impl;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.UniversalQueueMessage;

/**
 * Universal Kafka implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalQueueMessage}s.
 * </p>
 * 
 * <p>
 * Implementation: see {@link KafkaQueue}.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.2
 */
public class UniversalKafkaQueue extends KafkaQueue {

    /**
     * {@inheritDoc}
     */
    @Override
    protected byte[] serialize(IQueueMessage _msg) {
        if (_msg == null) {
            return null;
        }
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }

        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        return msg.toBytes();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalQueueMessage deserialize(byte[] msgData) {
        return UniversalQueueMessage.fromBytes(msgData);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalQueueMessage take() {
        return (UniversalQueueMessage) super.take();
    }
}

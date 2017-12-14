package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.RabbitMqQueue;
import com.github.ddth.queue.impl.universal.base.BaseUniversalRabbitMqQueue;
import com.github.ddth.queue.impl.universal.msg.UniversalIdIntQueueMessage;
import com.github.ddth.queue.utils.QueueException;

/**
 * (Experimental) Universal RabbitMQ implementation of {@link IQueue}.
 *
 * <p> Queue and Take {@link UniversalIdIntQueueMessage}s. </p>
 *
 * <p> Implementation: see {@link RabbitMqQueue}. </p>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.1
 */
public class UniversalRabbitMqQueue
        extends BaseUniversalRabbitMqQueue<UniversalIdIntQueueMessage, Long> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdIntQueueMessage deserialize(byte[] msgData) {
        try {
            return UniversalIdIntQueueMessage.fromBytes(msgData);
        } catch (Exception e) {
            throw new QueueException.CannotDeserializeQueueMessage(e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @since 0.6.0
     */
    @Override
    public UniversalIdIntQueueMessage createMessage() {
        return UniversalIdIntQueueMessage.newInstance();
    }

    /**
     * {@inheritDoc}
     *
     * @since 0.6.0
     */
    @Override
    public UniversalIdIntQueueMessage createMessage(byte[] data) {
        return UniversalIdIntQueueMessage.newInstance(data);
    }

    /**
     * {@inheritDoc}
     *
     * @since 0.6.0
     */
    @Override
    public UniversalIdIntQueueMessage createMessage(Long id, byte[] data) {
        return (UniversalIdIntQueueMessage) UniversalIdIntQueueMessage.newInstance(data).qId(id);
    }
}

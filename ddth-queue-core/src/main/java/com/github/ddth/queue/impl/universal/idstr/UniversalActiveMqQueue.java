package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.ActiveMqQueue;
import com.github.ddth.queue.impl.universal.base.BaseUniversalActiveMqQueue;
import com.github.ddth.queue.impl.universal.base.BaseUniversalQueueMessage;
import com.github.ddth.queue.impl.universal.msg.UniversalIdStrQueueMessage;
import com.github.ddth.queue.utils.QueueException;

/**
 * (Experimental) Universal ActiveMQ implementation of {@link IQueue}.
 *
 * <p>
 * Queue and Take {@link UniversalIdStrQueueMessage}s.
 * </p>
 *
 * <p>
 * Implementation: see {@link ActiveMqQueue}.
 * </p>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.1
 */
public class UniversalActiveMqQueue
        extends BaseUniversalActiveMqQueue<UniversalIdStrQueueMessage, String> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdStrQueueMessage deserialize(byte[] msgData) {
        try {
            return BaseUniversalQueueMessage.fromBytes(msgData, UniversalIdStrQueueMessage.class);
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
    public UniversalIdStrQueueMessage createMessage() {
        return UniversalIdStrQueueMessage.newInstance();
    }

    /**
     * {@inheritDoc}
     *
     * @since 0.6.0
     */
    @Override
    public UniversalIdStrQueueMessage createMessage(byte[] data) {
        return UniversalIdStrQueueMessage.newInstance(data);
    }

    /**
     * {@inheritDoc}
     *
     * @since 0.6.0
     */
    @Override
    public UniversalIdStrQueueMessage createMessage(String id, byte[] data) {
        return (UniversalIdStrQueueMessage) UniversalIdStrQueueMessage.newInstance(data).setId(id);
    }
}

package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.ActiveMqQueue;
import com.github.ddth.queue.impl.universal.BaseUniversalActiveMqQueue;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessageFactory;

/**
 * (Experimental) Universal ActiveMQ implementation of {@link IQueue}.
 *
 * <p>
 * Queue and Take {@link UniversalIdIntQueueMessage}s.
 * </p>
 *
 * <p>
 * Implementation: see {@link ActiveMqQueue}.
 * </p>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.1
 */
public class UniversalActiveMqQueue extends BaseUniversalActiveMqQueue<UniversalIdIntQueueMessage, Long> {
    /**
     * {@inheritDoc}
     *
     * @since 0.7.0
     */
    @Override
    public UniversalActiveMqQueue init() throws Exception {
        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdIntQueueMessageFactory.INSTANCE);
        }
        super.init();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdIntQueueMessage deserialize(byte[] msgData) {
        return deserialize(msgData, UniversalIdIntQueueMessage.class);
    }
}

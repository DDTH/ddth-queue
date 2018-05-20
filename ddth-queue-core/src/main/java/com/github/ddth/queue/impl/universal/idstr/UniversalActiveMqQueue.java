package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.ActiveMqQueue;
import com.github.ddth.queue.impl.universal.BaseUniversalActiveMqQueue;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessageFactory;

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
     * 
     * @since 0.7.0
     */
    @Override
    public UniversalActiveMqQueue init() throws Exception {
        super.init();

        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdStrQueueMessageFactory.INSTANCE);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdStrQueueMessage deserialize(byte[] msgData) {
        return deserialize(msgData, UniversalIdStrQueueMessage.class);
    }

}

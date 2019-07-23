package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.KafkaQueue;
import com.github.ddth.queue.impl.universal.BaseUniversalKafkaQueue;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessageFactory;

/**
 * (Experimental) Universal Kafka implementation of {@link IQueue}.
 *
 * <p>
 * Queue and Take {@link UniversalIdIntQueueMessage}s.
 * </p>
 *
 * <p>
 * Implementation: see {@link KafkaQueue}.
 * </p>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.2
 */
public class UniversalKafkaQueue extends BaseUniversalKafkaQueue<UniversalIdIntQueueMessage, Long> {
    /**
     * {@inheritDoc}
     *
     * @since 0.7.0
     */
    @Override
    public UniversalKafkaQueue init() throws Exception {
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

package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.KafkaQueue;
import com.github.ddth.queue.impl.base.BaseUniversalKafkaQueue;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.utils.QueueException;

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
     */
    @Override
    protected UniversalIdIntQueueMessage deserialize(byte[] msgData) {
        try {
            return UniversalIdIntQueueMessage.fromBytes(msgData);
        } catch (Exception e) {
            throw new QueueException.CannotDeserializeQueueMessage(e);
        }
    }
}

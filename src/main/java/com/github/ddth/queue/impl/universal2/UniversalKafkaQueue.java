package com.github.ddth.queue.impl.universal2;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.BaseUniversalKafkaQueue;
import com.github.ddth.queue.impl.KafkaQueue;
import com.github.ddth.queue.utils.QueueException;

/**
 * (Experimental) Universal Kafka implementation of {@link IQueue}.
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
 * @since 0.3.3
 */
public class UniversalKafkaQueue extends BaseUniversalKafkaQueue<UniversalQueueMessage> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalQueueMessage deserialize(byte[] msgData) {
        try {
            return UniversalQueueMessage.fromBytes(msgData);
        } catch (RuntimeException re) {
            throw new QueueException.CannotDeserializeQueueMessage(re);
        }
    }

}

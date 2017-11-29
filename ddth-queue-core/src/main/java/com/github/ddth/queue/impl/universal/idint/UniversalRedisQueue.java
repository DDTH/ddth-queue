package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.RedisQueue;
import com.github.ddth.queue.impl.base.BaseUniversalRedisQueue;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.utils.QueueException;

/**
 * Universal Redis implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalIdIntQueueMessage}s.
 * </p>
 * 
 * <p>
 * Implementation: see {@link RedisQueue}.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.0
 */
public class UniversalRedisQueue extends BaseUniversalRedisQueue<UniversalIdIntQueueMessage, Long> {

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

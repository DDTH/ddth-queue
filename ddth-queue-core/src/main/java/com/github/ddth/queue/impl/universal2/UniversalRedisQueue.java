package com.github.ddth.queue.impl.universal2;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.BaseUniversalRedisQueue;
import com.github.ddth.queue.impl.RedisQueue;
import com.github.ddth.queue.utils.QueueException;

/**
 * Universal Redis implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalQueueMessage}s.
 * </p>
 * 
 * <p>
 * Implementation: see {@link RedisQueue}.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.3
 */
public class UniversalRedisQueue extends BaseUniversalRedisQueue<UniversalQueueMessage> {
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

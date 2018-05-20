package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.RedisQueue;
import com.github.ddth.queue.impl.universal.BaseUniversalRedisQueue;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessageFactory;

/**
 * Universal Redis implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalIdStrQueueMessage}s.
 * </p>
 * 
 * <p>
 * Implementation: see {@link RedisQueue}.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.3
 */
public class UniversalRedisQueue
        extends BaseUniversalRedisQueue<UniversalIdStrQueueMessage, String> {

    /**
     * {@inheritDoc}
     * 
     * @since 0.7.0
     */
    @Override
    public UniversalRedisQueue init() throws Exception {
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

package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.RedisQueueFactory;

/**
 * Factory to create {@link UniversalRedisQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public class UniversalRedisQueueFactory
        extends RedisQueueFactory<UniversalRedisQueue, String, byte[]> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalRedisQueue createQueueInstance(final QueueSpec spec) {
        UniversalRedisQueue queue = new UniversalRedisQueue();
        return queue;
    }

}

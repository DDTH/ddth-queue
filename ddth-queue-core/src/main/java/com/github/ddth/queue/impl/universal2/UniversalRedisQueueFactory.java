package com.github.ddth.queue.impl.universal2;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.RedisQueueFactory;

/**
 * Factory to create {@link UniversalRedisQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public class UniversalRedisQueueFactory extends RedisQueueFactory<UniversalRedisQueue> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalRedisQueue createQueueInstance(final QueueSpec spec) {
        UniversalRedisQueue queue = new UniversalRedisQueue() {
            public void destroy() {
                disposeQueue(spec, this);
                super.destroy();
            }
        };
        return queue;
    }

}

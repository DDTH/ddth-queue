package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.MongodbQueueFactory;

/**
 * Factory to create {@link UniversalMongodbQueue} instances.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.1
 */
public class UniversalMongodbQueueFactory extends MongodbQueueFactory<UniversalMongodbQueue, String, byte[]> {
    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalMongodbQueue createQueueInstance(final QueueSpec spec) {
        UniversalMongodbQueue queue = new UniversalMongodbQueue();
        return queue;
    }
}

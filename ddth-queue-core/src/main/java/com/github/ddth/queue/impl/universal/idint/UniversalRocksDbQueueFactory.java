package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.RocksDbQueueFactory;

/**
 * Factory to create {@link UniversalRocksDbQueue} instances.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public class UniversalRocksDbQueueFactory extends RocksDbQueueFactory<UniversalRocksDbQueue, Long, byte[]> {
    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalRocksDbQueue createQueueInstance(final QueueSpec spec) {
        UniversalRocksDbQueue queue = new UniversalRocksDbQueue();
        return queue;
    }
}

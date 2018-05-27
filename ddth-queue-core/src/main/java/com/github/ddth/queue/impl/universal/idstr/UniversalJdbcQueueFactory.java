package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.JdbcQueueFactory;

/**
 * Factory to create {@link UniversalJdbcQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public class UniversalJdbcQueueFactory
        extends JdbcQueueFactory<UniversalJdbcQueue, String, byte[]> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalJdbcQueue createQueueInstance(final QueueSpec spec) {
        UniversalJdbcQueue queue = new UniversalJdbcQueue();
        return queue;
    }

}

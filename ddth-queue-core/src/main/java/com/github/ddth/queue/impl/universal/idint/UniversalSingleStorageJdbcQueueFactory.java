package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.JdbcQueueFactory;

/**
 * Factory to create {@link UniversalJdbcQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public class UniversalSingleStorageJdbcQueueFactory
        extends JdbcQueueFactory<UniversalSingleStorageJdbcQueue, Long, byte[]> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalSingleStorageJdbcQueue createQueueInstance(final QueueSpec spec) {
        UniversalSingleStorageJdbcQueue queue = new UniversalSingleStorageJdbcQueue() {
            private boolean destroyed = false;

            public void destroy() {
                if (!destroyed) {
                    destroyed = true;
                    disposeQueue(spec, this);
                    super.destroy();
                }
            }
        };
        return queue;
    }

}
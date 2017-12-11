package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.JdbcQueueFactory;

/**
 * Factory to create {@link LessLockingUniversalSingleStorageMySQLQueue}
 * instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.0
 */
public class LessLockingUniversalSingleStorageMySQLQueueFactory
        extends JdbcQueueFactory<LessLockingUniversalSingleStorageMySQLQueue, Long, byte[]> {

    public final static String SPEC_FIELD_FIFO = "fifo";

    /**
     * {@inheritDoc}
     */
    @Override
    protected LessLockingUniversalSingleStorageMySQLQueue createQueueInstance(
            final QueueSpec spec) {
        LessLockingUniversalSingleStorageMySQLQueue queue = new LessLockingUniversalSingleStorageMySQLQueue() {
            private boolean destroyed = false;

            public void destroy() {
                if (!destroyed) {
                    destroyed = true;
                    disposeQueue(spec, this);
                    super.destroy();
                }
            }
        };
        Boolean fifo = spec.getField(SPEC_FIELD_FIFO, Boolean.class);
        if (fifo != null) {
            queue.setFifo(fifo.booleanValue());
        }
        return queue;
    }

}

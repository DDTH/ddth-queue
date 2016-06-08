package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.JdbcQueueFactory;

/**
 * Factory to create {@link LessLockingUniversalPgSQLQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public class LessLockingUniversalPgSQLQueueFactory
        extends JdbcQueueFactory<LessLockingUniversalPgSQLQueue> {

    public final static String SPEC_FIELD_FIFO = "fifo";

    /**
     * {@inheritDoc}
     */
    @Override
    protected LessLockingUniversalPgSQLQueue createQueueInstance(final QueueSpec spec) {
        LessLockingUniversalPgSQLQueue queue = new LessLockingUniversalPgSQLQueue() {
            public void destroy() {
                disposeQueue(spec, this);
                super.destroy();
            }
        };
        Boolean fifo = spec.getField(SPEC_FIELD_FIFO, Boolean.class);
        if (fifo != null) {
            queue.setFifo(fifo.booleanValue());
        }
        return queue;
    }

}

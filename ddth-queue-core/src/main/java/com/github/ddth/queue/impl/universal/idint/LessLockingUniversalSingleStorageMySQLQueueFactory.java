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
    private boolean defaultFifo = true;

    /**
     * @return
     * @since 0.6.2
     */
    public boolean isDefaultFifo() {
        return defaultFifo;
    }

    /**
     * @return
     * @since 0.6.2
     */
    public boolean getDefaultFifo() {
        return defaultFifo;
    }

    /**
     * @param defaultFifo
     * @since 0.6.2
     */
    public void setDefaultFifo(boolean defaultFifo) {
        this.defaultFifo = defaultFifo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected LessLockingUniversalSingleStorageMySQLQueue createQueueInstance(final QueueSpec spec) {
        LessLockingUniversalSingleStorageMySQLQueue queue = new LessLockingUniversalSingleStorageMySQLQueue();
        queue.setFifo(defaultFifo);
        Boolean fifo = spec.getField(SPEC_FIELD_FIFO, Boolean.class);
        if (fifo != null) {
            queue.setFifo(fifo.booleanValue());
        }
        return queue;
    }
}

package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.InmemQueueFactory;

/**
 * Factory to create {@link UniversalInmemQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public class UniversalInmemQueueFactory
        extends InmemQueueFactory<UniversalInmemQueue, Long, byte[]> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalInmemQueue createQueueInstance(final QueueSpec spec) {
        UniversalInmemQueue queue = new UniversalInmemQueue() {
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

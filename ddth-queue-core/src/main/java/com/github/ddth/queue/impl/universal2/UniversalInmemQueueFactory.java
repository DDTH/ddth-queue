package com.github.ddth.queue.impl.universal2;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.InmemQueueFactory;

/**
 * Factory to create {@link UniversalInmemQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public class UniversalInmemQueueFactory extends InmemQueueFactory<UniversalInmemQueue> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalInmemQueue createQueueInstance(final QueueSpec spec) {
        UniversalInmemQueue queue = new UniversalInmemQueue() {
            public void destroy() {
                disposeQueue(spec, this);
                super.destroy();
            }
        };
        return queue;
    }

}

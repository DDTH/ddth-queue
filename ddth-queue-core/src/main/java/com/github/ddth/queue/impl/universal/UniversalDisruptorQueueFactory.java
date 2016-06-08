package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.DisruptorQueueFactory;

/**
 * Factory to create {@link UniversalDisruptorQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public class UniversalDisruptorQueueFactory extends DisruptorQueueFactory<UniversalDisruptorQueue> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalDisruptorQueue createQueueInstance(final QueueSpec spec) {
        UniversalDisruptorQueue queue = new UniversalDisruptorQueue() {
            public void destroy() {
                disposeQueue(spec, this);
                super.destroy();
            }
        };
        return queue;
    }

}

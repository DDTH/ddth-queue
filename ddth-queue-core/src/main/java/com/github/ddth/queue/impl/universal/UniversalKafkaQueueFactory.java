package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.KafkaQueueFactory;

/**
 * Factory to create {@link UniversalKafkaQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public class UniversalKafkaQueueFactory extends KafkaQueueFactory<UniversalKafkaQueue> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalKafkaQueue createQueueInstance(final QueueSpec spec) {
        UniversalKafkaQueue queue = new UniversalKafkaQueue() {
            public void destroy() {
                disposeQueue(spec, this);
                super.destroy();
            }
        };
        return queue;
    }

}

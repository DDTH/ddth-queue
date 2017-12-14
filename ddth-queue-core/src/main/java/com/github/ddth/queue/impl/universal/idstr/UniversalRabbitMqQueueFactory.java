package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.RabbitMqQueueFactory;

/**
 * Factory to create {@link UniversalRabbitMqQueue} instances.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.1
 */
public class UniversalRabbitMqQueueFactory
        extends RabbitMqQueueFactory<UniversalRabbitMqQueue, String, byte[]> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalRabbitMqQueue createQueueInstance(final QueueSpec spec) {
        UniversalRabbitMqQueue queue = new UniversalRabbitMqQueue() {
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

package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.RabbitMqQueueFactory;

/**
 * Factory to create {@link UniversalRabbitMqQueue} instances.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.1
 */
public class UniversalRabbitMqQueueFactory extends RabbitMqQueueFactory<UniversalRabbitMqQueue, Long, byte[]> {
    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalRabbitMqQueue createQueueInstance(final QueueSpec spec) {
        UniversalRabbitMqQueue queue = new UniversalRabbitMqQueue();
        return queue;
    }
}

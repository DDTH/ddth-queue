package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.universal.BaseUniversalDisruptorQueue;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessage;

/**
 * Universal LMAX Disruptor implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalIdStrQueueMessage}s.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class UniversalDisruptorQueue
        extends BaseUniversalDisruptorQueue<UniversalIdStrQueueMessage, String> {

    public UniversalDisruptorQueue() {
    }

    public UniversalDisruptorQueue(int ringSize) {
        super(ringSize);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.0
     */
    @Override
    public UniversalIdStrQueueMessage createMessage() {
        return UniversalIdStrQueueMessage.newInstance();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.0
     */
    @Override
    public UniversalIdStrQueueMessage createMessage(byte[] data) {
        return UniversalIdStrQueueMessage.newInstance(data);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.0
     */
    @Override
    public UniversalIdStrQueueMessage createMessage(String id, byte[] data) {
        return (UniversalIdStrQueueMessage) UniversalIdStrQueueMessage.newInstance(data).setId(id);
    }
}

package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.universal.BaseUniversalDisruptorQueue;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessageFactory;

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
     * @since 0.7.0
     */
    @Override
    public UniversalDisruptorQueue init() throws Exception {
        super.init();

        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdStrQueueMessageFactory.INSTANCE);
        }

        return this;
    }

}

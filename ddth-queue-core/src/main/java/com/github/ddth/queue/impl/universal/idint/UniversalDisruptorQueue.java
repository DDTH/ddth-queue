package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.base.BaseUniversalDisruptorQueue;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;

/**
 * Universal LMAX Disruptor implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalIdIntQueueMessage}s.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class UniversalDisruptorQueue
        extends BaseUniversalDisruptorQueue<UniversalIdIntQueueMessage, Long> {

    public UniversalDisruptorQueue() {
    }

    public UniversalDisruptorQueue(int ringSize) {
        super(ringSize);
    }
}
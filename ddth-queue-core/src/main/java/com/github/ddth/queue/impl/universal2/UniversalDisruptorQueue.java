package com.github.ddth.queue.impl.universal2;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.BaseUniversalDisruptorQueue;

/**
 * Universal LMAX Disruptor implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalQueueMessage}s.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class UniversalDisruptorQueue extends BaseUniversalDisruptorQueue<UniversalQueueMessage> {
    public UniversalDisruptorQueue() {
    }

    public UniversalDisruptorQueue(int ringSize) {
        super(ringSize);
    }
}

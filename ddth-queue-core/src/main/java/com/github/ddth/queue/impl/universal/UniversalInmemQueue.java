package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.base.BaseUniversalInmemQueue;

/**
 * Universal in-memory implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalQueueMessage}s.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class UniversalInmemQueue extends BaseUniversalInmemQueue<UniversalQueueMessage> {
    public UniversalInmemQueue() {
    }

    public UniversalInmemQueue(int boundary) {
        super(boundary);
    }
}

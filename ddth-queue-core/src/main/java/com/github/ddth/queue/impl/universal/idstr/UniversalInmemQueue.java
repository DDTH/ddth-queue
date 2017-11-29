package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.base.BaseUniversalInmemQueue;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessage;

/**
 * Universal in-memory implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalIdStrQueueMessage}s.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class UniversalInmemQueue
        extends BaseUniversalInmemQueue<UniversalIdStrQueueMessage, String> {
    public UniversalInmemQueue() {
    }

    public UniversalInmemQueue(int boundary) {
        super(boundary);
    }
}

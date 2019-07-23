package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.universal.BaseUniversalInmemQueue;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessageFactory;

/**
 * Universal in-memory implementation of {@link IQueue}.
 *
 * <p>
 * Queue and Take {@link UniversalIdIntQueueMessage}s.
 * </p>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class UniversalInmemQueue extends BaseUniversalInmemQueue<UniversalIdIntQueueMessage, Long> {
    public UniversalInmemQueue() {
    }

    public UniversalInmemQueue(int boundary) {
        super(boundary);
    }

    /**
     * {@inheritDoc}
     *
     * @since 0.7.0
     */
    @Override
    public UniversalInmemQueue init() throws Exception {
        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdIntQueueMessageFactory.INSTANCE);
        }
        super.init();
        return this;
    }
}

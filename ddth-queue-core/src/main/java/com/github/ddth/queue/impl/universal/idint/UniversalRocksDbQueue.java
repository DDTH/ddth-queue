package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.RocksDbQueue;
import com.github.ddth.queue.impl.universal.BaseUniversalRocksDbQueue;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessageFactory;

/**
 * Universal RocskDB implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalIdIntQueueMessage}s.
 * </p>
 * 
 * <p>
 * Implementation: see {@link RocksDbQueue}.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class UniversalRocksDbQueue
        extends BaseUniversalRocksDbQueue<UniversalIdIntQueueMessage, Long> {

    /**
     * {@inheritDoc}
     * 
     * @since 0.7.0
     */
    @Override
    public UniversalRocksDbQueue init() throws Exception {
        super.init();

        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdIntQueueMessageFactory.INSTANCE);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdIntQueueMessage deserialize(byte[] msgData) {
        return deserialize(msgData, UniversalIdIntQueueMessage.class);
    }
}

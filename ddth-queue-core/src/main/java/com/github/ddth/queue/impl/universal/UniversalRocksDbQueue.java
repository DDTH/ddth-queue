package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.BaseUniversalRocksDbQueue;
import com.github.ddth.queue.impl.RocksDbQueue;
import com.github.ddth.queue.utils.QueueException;

/**
 * Universal RocskDB implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalQueueMessage}s.
 * </p>
 * 
 * <p>
 * Implementation: see {@link RocksDbQueue}.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class UniversalRocksDbQueue extends BaseUniversalRocksDbQueue<UniversalQueueMessage> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalQueueMessage deserialize(byte[] msgData) {
        try {
            return UniversalQueueMessage.fromBytes(msgData);
        } catch (RuntimeException re) {
            throw new QueueException.CannotDeserializeQueueMessage(re);
        }
    }
}

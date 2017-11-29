package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.RocksDbQueue;
import com.github.ddth.queue.impl.base.BaseUniversalRocksDbQueue;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.utils.QueueException;

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
     */
    @Override
    protected UniversalIdIntQueueMessage deserialize(byte[] msgData) {
        try {
            return UniversalIdIntQueueMessage.fromBytes(msgData);
        } catch (Exception e) {
            throw new QueueException.CannotDeserializeQueueMessage(e);
        }
    }
}

package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.RocksDbQueue;
import com.github.ddth.queue.impl.base.BaseUniversalRocksDbQueue;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessage;
import com.github.ddth.queue.utils.QueueException;

/**
 * Universal RocskDB implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalIdStrQueueMessage}s.
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
        extends BaseUniversalRocksDbQueue<UniversalIdStrQueueMessage, String> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdStrQueueMessage deserialize(byte[] msgData) {
        try {
            return UniversalIdStrQueueMessage.fromBytes(msgData);
        } catch (Exception e) {
            throw new QueueException.CannotDeserializeQueueMessage(e);
        }
    }
}

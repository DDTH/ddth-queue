package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.RocksDbQueue;
import com.github.ddth.queue.impl.universal.BaseUniversalQueueMessage;
import com.github.ddth.queue.impl.universal.BaseUniversalRocksDbQueue;
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
     * 
     * @since 0.6.0
     */
    @Override
    public UniversalIdIntQueueMessage createMessage() {
        return UniversalIdIntQueueMessage.newInstance();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.0
     */
    @Override
    public UniversalIdIntQueueMessage createMessage(byte[] data) {
        return UniversalIdIntQueueMessage.newInstance(data);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.0
     */
    @Override
    public UniversalIdIntQueueMessage createMessage(Long id, byte[] data) {
        return (UniversalIdIntQueueMessage) UniversalIdIntQueueMessage.newInstance(data).setId(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdIntQueueMessage deserialize(byte[] msgData) {
        try {
            return BaseUniversalQueueMessage.fromBytes(msgData, UniversalIdIntQueueMessage.class);
        } catch (Exception e) {
            throw new QueueException.CannotDeserializeQueueMessage(e);
        }
    }
}

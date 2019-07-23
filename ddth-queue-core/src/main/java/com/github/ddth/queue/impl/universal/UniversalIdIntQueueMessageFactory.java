package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.IQueueMessageFactory;

/**
 * Factory that creates {@link UniversalIdIntQueueMessage}s.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class UniversalIdIntQueueMessageFactory implements IQueueMessageFactory<Long, byte[]> {
    public final static UniversalIdIntQueueMessageFactory INSTANCE = new UniversalIdIntQueueMessageFactory();

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdIntQueueMessage createMessage() {
        return UniversalIdIntQueueMessage.newInstance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdIntQueueMessage createMessage(byte[] data) {
        return UniversalIdIntQueueMessage.newInstance(data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdIntQueueMessage createMessage(Long id, byte[] data) {
        return UniversalIdIntQueueMessage.newInstance(id, data);
    }
}

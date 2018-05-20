package com.github.ddth.pubsub.impl.universal;

import com.github.ddth.queue.IMessageFactory;

/**
 * Factory that creates {@link UniversalIdIntMessage}s.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class UniversalIdIntMessageFactory implements IMessageFactory<Long, byte[]> {

    public final static UniversalIdIntMessageFactory INSTANCE = new UniversalIdIntMessageFactory();

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdIntMessage createMessage() {
        return UniversalIdIntMessage.newInstance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdIntMessage createMessage(byte[] data) {
        return UniversalIdIntMessage.newInstance(data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdIntMessage createMessage(Long id, byte[] data) {
        return UniversalIdIntMessage.newInstance(id, data);
    }

}

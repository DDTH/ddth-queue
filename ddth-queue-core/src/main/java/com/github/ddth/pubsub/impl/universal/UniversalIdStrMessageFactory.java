package com.github.ddth.pubsub.impl.universal;

import com.github.ddth.queue.IMessageFactory;

/**
 * Factory that creates {@link UniversalIdStrMessage}s.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class UniversalIdStrMessageFactory implements IMessageFactory<String, byte[]> {
    public final static UniversalIdStrMessageFactory INSTANCE = new UniversalIdStrMessageFactory();

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdStrMessage createMessage() {
        return UniversalIdStrMessage.newInstance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdStrMessage createMessage(byte[] data) {
        return UniversalIdStrMessage.newInstance(data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdStrMessage createMessage(String id, byte[] data) {
        return UniversalIdStrMessage.newInstance(id, data);
    }
}

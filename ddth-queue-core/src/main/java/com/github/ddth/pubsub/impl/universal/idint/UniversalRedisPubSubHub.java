package com.github.ddth.pubsub.impl.universal.idint;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.impl.RedisPubSubHub;
import com.github.ddth.pubsub.impl.universal.UniversalIdIntMessage;
import com.github.ddth.pubsub.impl.universal.UniversalIdIntMessageFactory;

/**
 * Universal Redis implementation of {@link IPubSubHub}.
 *
 * <p>
 * Message type: {@link UniversalIdIntMessage}.
 * </p>
 *
 * <p>
 * Implementation: see {@link RedisPubSubHub}.
 * </p>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class UniversalRedisPubSubHub extends RedisPubSubHub<Long, byte[]> {
    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalRedisPubSubHub init() {
        super.init();

        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdIntMessageFactory.INSTANCE);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdIntMessage deserialize(byte[] msgData) {
        return deserialize(msgData, UniversalIdIntMessage.class);
    }
}

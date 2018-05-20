package com.github.ddth.pubsub.impl.universal.idstr;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.impl.RedisPubSubHub;
import com.github.ddth.pubsub.impl.universal.UniversalIdIntMessage;
import com.github.ddth.pubsub.impl.universal.UniversalIdStrMessage;
import com.github.ddth.pubsub.impl.universal.UniversalIdStrMessageFactory;

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
public class UniversalRedisPubSubHub extends RedisPubSubHub<String, byte[]> {
    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalRedisPubSubHub init() {
        super.init();

        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdStrMessageFactory.INSTANCE);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdStrMessage deserialize(byte[] msgData) {
        return deserialize(msgData, UniversalIdStrMessage.class);
    }
}

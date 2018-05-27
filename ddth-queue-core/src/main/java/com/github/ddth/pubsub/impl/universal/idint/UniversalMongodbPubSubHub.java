package com.github.ddth.pubsub.impl.universal.idint;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.impl.MongodbPubSubHub;
import com.github.ddth.pubsub.impl.universal.UniversalIdIntMessage;
import com.github.ddth.pubsub.impl.universal.UniversalIdIntMessageFactory;

/**
 * Universal MongoDB implementation of {@link IPubSubHub}.
 *
 * <p>
 * Message type: {@link UniversalIdIntMessage}.
 * </p>
 *
 * <p>
 * Implementation: see {@link MongodbPubSubHub}.
 * </p>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.1
 */
public class UniversalMongodbPubSubHub extends MongodbPubSubHub<Long, byte[]> {
    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalMongodbPubSubHub init() {
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

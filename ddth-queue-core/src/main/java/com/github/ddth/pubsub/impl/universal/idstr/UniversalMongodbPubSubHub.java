package com.github.ddth.pubsub.impl.universal.idstr;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.impl.MongodbPubSubHub;
import com.github.ddth.pubsub.impl.universal.UniversalIdStrMessage;
import com.github.ddth.pubsub.impl.universal.UniversalIdStrMessageFactory;

/**
 * Universal MongoDB implementation of {@link IPubSubHub}.
 *
 * <p>
 * Message type: {@link UniversalIdStrMessage}.
 * </p>
 *
 * <p>
 * Implementation: see {@link MongodbPubSubHub}.
 * </p>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.1
 */
public class UniversalMongodbPubSubHub extends MongodbPubSubHub<String, byte[]> {
    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalMongodbPubSubHub init() {
        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdStrMessageFactory.INSTANCE);
        }
        super.init();
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

package com.github.ddth.pubsub.impl.universal.idstr;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.impl.InmemPubSubHub;
import com.github.ddth.pubsub.impl.universal.UniversalIdStrMessage;
import com.github.ddth.pubsub.impl.universal.UniversalIdStrMessageFactory;

/**
 * Universal in-memory implementation of {@link IPubSubHub}.
 *
 * <p>
 * Message type: {@link UniversalIdStrMessage}.
 * </p>
 *
 * <p>
 * Implementation: see {@link InmemPubSubHub}.
 * </p>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class UniversalInmemPubSubHub extends InmemPubSubHub<String, byte[]> {
    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalInmemPubSubHub init() {
        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdStrMessageFactory.INSTANCE);
        }
        super.init();
        return this;
    }
}

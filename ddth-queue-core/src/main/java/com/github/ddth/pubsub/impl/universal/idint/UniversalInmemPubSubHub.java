package com.github.ddth.pubsub.impl.universal.idint;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.impl.InmemPubSubHub;
import com.github.ddth.pubsub.impl.universal.UniversalIdIntMessage;
import com.github.ddth.pubsub.impl.universal.UniversalIdIntMessageFactory;

/**
 * Universal in-memory implementation of {@link IPubSubHub}.
 *
 * <p>
 * Message type: {@link UniversalIdIntMessage}.
 * </p>
 *
 * <p>
 * Implementation: see {@link InmemPubSubHub}.
 * </p>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class UniversalInmemPubSubHub extends InmemPubSubHub<Long, byte[]> {
    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalInmemPubSubHub init() {
        super.init();

        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdIntMessageFactory.INSTANCE);
        }

        return this;
    }
}

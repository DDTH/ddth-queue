package com.github.ddth.pubsub.impl.universal;

import com.github.ddth.pubsub.ISubscriber;
import com.github.ddth.queue.IMessage;

/**
 * A subscriber that accepts only {@link UniversalIdIntMessage}s.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public abstract class UniversalIdIntSubscriber implements ISubscriber<Long, byte[]> {

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onMessage(String channel, IMessage<Long, byte[]> msg) {
        if (msg instanceof UniversalIdIntMessage) {
            return onMessage(channel, (UniversalIdIntMessage) msg);
        }
        throw new IllegalArgumentException("This subscriber expects message of type ["
                + UniversalIdIntMessage.class.getName() + "]!");
    }

    /**
     * Sub-class override this method to implement its own business logic.
     * 
     * @param channel
     * @param msg
     * @return
     */
    public abstract boolean onMessage(String channel, UniversalIdIntMessage msg);

}

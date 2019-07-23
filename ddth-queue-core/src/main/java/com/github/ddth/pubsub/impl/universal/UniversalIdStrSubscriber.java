package com.github.ddth.pubsub.impl.universal;

import com.github.ddth.pubsub.ISubscriber;
import com.github.ddth.queue.IMessage;

/**
 * A subscriber that accepts only {@link UniversalIdStrMessage}s.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public abstract class UniversalIdStrSubscriber implements ISubscriber<String, byte[]> {
    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onMessage(String channel, IMessage<String, byte[]> msg) {
        if (msg instanceof UniversalIdStrMessage) {
            return onMessage(channel, (UniversalIdStrMessage) msg);
        }
        throw new IllegalArgumentException(
                "This subscriber expects message of type [" + UniversalIdStrMessage.class.getName() + "] but received ["
                        + msg.getClass().getName() + "].");
    }

    /**
     * Sub-class override this method to implement its own business logic.
     *
     * @param channel
     * @param msg
     * @return
     */
    public abstract boolean onMessage(String channel, UniversalIdStrMessage msg);
}

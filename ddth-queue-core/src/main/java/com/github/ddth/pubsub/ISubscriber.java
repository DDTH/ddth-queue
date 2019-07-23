package com.github.ddth.pubsub;

import com.github.ddth.queue.IMessage;

/**
 * API interface used to subscribe to message channels.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public interface ISubscriber<ID, DATA> {
    /**
     * Called when a message arrives on a channel.
     * 
     * @param channel
     * @param msg
     * @return
     */
    boolean onMessage(String channel, IMessage<ID, DATA> msg);
}

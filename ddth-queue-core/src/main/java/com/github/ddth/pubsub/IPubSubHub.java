package com.github.ddth.pubsub;

import com.github.ddth.queue.IMessage;

/**
 * API interface to publish messages and subscribe to channels for messages.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public interface IPubSubHub<ID, DATA> {
    /**
     * Create a new, empty message.
     *
     * @return
     */
    IMessage<ID, DATA> createMessage();

    /**
     * Create a new message, supplying its initial data.
     *
     * @param data
     * @return
     */
    default IMessage<ID, DATA> createMessage(DATA data) {
        return createMessage().setData(data);
    }

    /**
     * Create a new message, supplying its initial id and data.
     *
     * @param id
     * @param data
     * @return
     */
    default IMessage<ID, DATA> createMessage(ID id, DATA data) {
        return createMessage().setId(id).setData(data);
    }

    /**
     * Publish a message to a channel.
     *
     * @param channel
     * @param msg
     * @return
     */
    boolean publish(String channel, IMessage<ID, DATA> msg);

    /**
     * Subscribe to a channel for messages.
     *
     * @param channel
     * @param subscriber
     */
    void subscribe(String channel, ISubscriber<ID, DATA> subscriber);

    /**
     * Unsubscribe from a channel.
     *
     * @param channel
     * @param subscriber
     */
    void unsubscribe(String channel, ISubscriber<ID, DATA> subscriber);
}

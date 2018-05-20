package com.github.ddth.queue;

/**
 * Factory to create {@link IMessage}s.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public interface IMessageFactory<ID, DATA> {
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
    IMessage<ID, DATA> createMessage(DATA data);
}

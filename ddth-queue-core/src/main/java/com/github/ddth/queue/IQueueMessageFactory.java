package com.github.ddth.queue;

/**
 * Factory to create {@link IQueueMessage}s.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public interface IQueueMessageFactory<ID, DATA> {
    /**
     * Create a new, empty queue message.
     *
     * @return
     */
    IQueueMessage<ID, DATA> createMessage();

    /**
     * Create a new queue message, supplying its initial data.
     *
     * @param content
     * @return
     */
    default IQueueMessage<ID, DATA> createMessage(DATA content) {
        return (IQueueMessage<ID, DATA>) createMessage().setData(content);
    }

    /**
     * Create a new queue message, supplying its initial id and data.
     *
     * @param id
     * @param content
     * @return
     */
    default IQueueMessage<ID, DATA> createMessage(ID id, DATA content) {
        return (IQueueMessage<ID, DATA>) createMessage().setId(id).setData(content);
    }
}

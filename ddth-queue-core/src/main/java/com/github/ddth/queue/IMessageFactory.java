package com.github.ddth.queue;

import com.github.ddth.queue.IMessage.EmptyMessage;

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

    /*----------------------------------------------------------------------*/

    @SuppressWarnings("rawtypes")
    class EmptyMessageFactory implements IMessageFactory {
        public final static EmptyMessageFactory INSTANCE = new EmptyMessageFactory();

        @Override
        public IMessage createMessage() {
            return EmptyMessage.INSTANCE;
        }

        @Override
        public IMessage createMessage(Object data) {
            return EmptyMessage.INSTANCE;
        }

        @Override
        public IMessage createMessage(Object id, Object data) {
            return EmptyMessage.INSTANCE;
        }
    }
}

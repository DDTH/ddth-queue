package com.github.ddth.queue;

import java.util.Date;

/**
 * Represent a message.
 * 
 * <p>
 * A message has 3 base attributes: id, data and timestamp.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public interface IMessage<ID, DATA> extends Cloneable {

    /**
     * Clone this message.
     * 
     * @return
     */
    IMessage<ID, DATA> clone();

    /**
     * Message's unique id in queue.
     * 
     * @return
     */
    ID getId();

    /**
     * Set message's unique queue id.
     * 
     * @param id
     * @return
     */
    IMessage<ID, DATA> setId(ID id);

    /**
     * Message's timestamp.
     * 
     * @return
     */
    Date getTimestamp();

    /**
     * Set message's timestamp.
     * 
     * @param timestamp
     * @return
     */
    IMessage<ID, DATA> setTimestamp(Date timestamp);

    /**
     * Data/content attached to the queue message.
     * 
     * @return
     */
    DATA getData();

    /**
     * Attach data/content to the queue message.
     * 
     * @param data
     * @return
     */
    IMessage<ID, DATA> setData(DATA data);

    /**
     * An empty message.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     */
    @SuppressWarnings("rawtypes")
    static class EmptyMessage implements IMessage {
        public final static EmptyMessage INSTANCE = new EmptyMessage();

        /**
         * {@inheritDoc}
         */
        @Override
        public EmptyMessage clone() {
            try {
                return (EmptyMessage) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Object getId() {
            return null;
        }

        @Override
        public EmptyMessage setId(Object queueId) {
            return this;
        }

        @Override
        public Date getTimestamp() {
            return null;
        }

        @Override
        public EmptyMessage setTimestamp(Date timestamp) {
            return this;
        }

        @Override
        public Object getData() {
            return null;
        }

        @Override
        public EmptyMessage setData(Object data) {
            return this;
        }
    }
}

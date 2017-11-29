package com.github.ddth.queue;

import java.util.Date;

/**
 * Represents a queue message.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public interface IQueueMessage<ID, DATA> extends Cloneable {

    /**
     * Clone this message.
     * 
     * @return
     * @since 0.4.0
     */
    IQueueMessage<ID, DATA> clone();

    /**
     * Message's unique id in queue.
     * 
     * @return
     */
    ID qId();

    /**
     * Set message's unique queue id.
     * 
     * @param id
     * @return
     */
    IQueueMessage<ID, DATA> qId(ID queueId);

    /**
     * Message's first-queued timestamp.
     * 
     * @return
     */
    Date qOriginalTimestamp();

    /**
     * Set message's first-queued timestamp.
     * 
     * @param timestamp
     * @return
     */
    IQueueMessage<ID, DATA> qOriginalTimestamp(Date timestamp);

    /**
     * Message's last-queued timestamp.
     * 
     * @return
     */
    Date qTimestamp();

    /**
     * Set message's last-queued timestamp.
     * 
     * @param timestamp
     * @return
     */
    IQueueMessage<ID, DATA> qTimestamp(Date timestamp);

    /**
     * How many times message has been re-queued?
     * 
     * @return
     */
    int qNumRequeues();

    /**
     * Set message's number of re-queue times.
     * 
     * @param numRequeues
     * @return
     */
    IQueueMessage<ID, DATA> qNumRequeues(int numRequeues);

    /**
     * Increase message's number of re-queue times by 1.
     * 
     * @return
     */
    IQueueMessage<ID, DATA> qIncNumRequeues();

    /**
     * Data/content attached to the queue message.
     * 
     * @return
     * @since 0.4.2
     */
    DATA qData();

    /**
     * Attach data/content to the queue message.
     * 
     * @param data
     * @return
     * @since 0.4.2
     */
    IQueueMessage<ID, DATA> qData(DATA data);

    /**
     * An empty queue message.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.3.3
     */
    static class EmptyQueueMessage implements IQueueMessage<Object, Object> {
        /**
         * {@inheritDoc}
         */
        @Override
        public EmptyQueueMessage clone() {
            try {
                return (EmptyQueueMessage) super.clone();
            } catch (CloneNotSupportedException e) {
                throw new RuntimeException(e);
            }
        }

        @Override
        public Object qId() {
            return null;
        }

        @Override
        public EmptyQueueMessage qId(Object queueId) {
            return this;
        }

        @Override
        public Date qOriginalTimestamp() {
            return null;
        }

        @Override
        public EmptyQueueMessage qOriginalTimestamp(Date timestamp) {
            return this;
        }

        @Override
        public Date qTimestamp() {
            return null;
        }

        @Override
        public EmptyQueueMessage qTimestamp(Date timestamp) {
            return this;
        }

        @Override
        public int qNumRequeues() {
            return 0;
        }

        @Override
        public EmptyQueueMessage qNumRequeues(int numRequeues) {
            return null;
        }

        @Override
        public EmptyQueueMessage qIncNumRequeues() {
            return this;
        }

        @Override
        public Object qData() {
            return null;
        }

        @Override
        public EmptyQueueMessage qData(Object data) {
            return this;
        }
    }
}

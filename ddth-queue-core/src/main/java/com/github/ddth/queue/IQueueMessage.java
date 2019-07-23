package com.github.ddth.queue;

import java.util.Date;

/**
 * Represents a queue message.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public interface IQueueMessage<ID, DATA> extends IMessage<ID, DATA> {
    /**
     * Clone this message.
     *
     * @return
     * @since 0.4.0
     */
    IQueueMessage<ID, DATA> clone();

    /**
     * Get message's last-queued timestamp.
     *
     * @return
     * @since 0.7.0
     */
    Date getQueueTimestamp();

    /**
     * Set message's last-queued timestamp.
     *
     * @param timestamp
     * @return
     * @since 0.7.0
     */
    IQueueMessage<ID, DATA> setQueueTimestamp(Date timestamp);

    /**
     * How many times message has been re-queued?
     *
     * @return
     * @since 0.7.0
     */
    int getNumRequeues();

    /**
     * Set message's number of re-queue times.
     *
     * @param numRequeues
     * @return
     * @since 0.7.0
     */
    IQueueMessage<ID, DATA> setNumRequeues(int numRequeues);

    /**
     * Increase message's number of re-queue times by 1.
     *
     * @return
     * @since 0.7.0
     */
    IQueueMessage<ID, DATA> incNumRequeues();

    /**
     * An empty queue message.
     *
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.3.3
     */
    @SuppressWarnings("rawtypes")
    class EmptyQueueMessage extends EmptyMessage implements IQueueMessage {
        /**
         * {@inheritDoc}
         */
        @Override
        public EmptyQueueMessage clone() {
            return (EmptyQueueMessage) super.clone();
        }

        private Date now = new Date();

        @Override
        public Date getQueueTimestamp() {
            return now;
        }

        @Override
        public EmptyQueueMessage setQueueTimestamp(Date timestamp) {
            return this;
        }

        @Override
        public int getNumRequeues() {
            return 0;
        }

        @Override
        public EmptyQueueMessage setNumRequeues(int EmptyQueueMessage) {
            return this;
        }

        @Override
        public EmptyQueueMessage incNumRequeues() {
            return this;
        }
    }
}

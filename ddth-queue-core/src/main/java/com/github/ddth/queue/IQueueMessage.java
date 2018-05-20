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
     * Message's unique id in queue.
     * 
     * @return
     * @deprecated since v0.7.0 use {@link #getId()}.
     */
    ID qId();

    /**
     * Set message's unique queue id.
     * 
     * @param id
     * @return
     * @deprecated since v0.7.0 use {@link #setId(Object)}.
     */
    IQueueMessage<ID, DATA> qId(ID queueId);

    /**
     * Message's first-queued timestamp.
     * 
     * @return
     * @deprecated since v0.7.0 use {@link #getTimestamp()}.
     */
    Date qOriginalTimestamp();

    /**
     * Set message's first-queued timestamp.
     * 
     * @param timestamp
     * @return
     * @deprecated since v0.7.0 use {@link #setTimestamp(Date)}.
     */
    IQueueMessage<ID, DATA> qOriginalTimestamp(Date timestamp);

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
     * Message's last-queued timestamp.
     * 
     * @return
     * @deprecated since v0.7.0 use {@link #getQueueTimestamp()}.
     */
    Date qTimestamp();

    /**
     * Set message's last-queued timestamp.
     * 
     * @param timestamp
     * @return
     * @deprecated since v0.7.0 use {@link #setQueueTimestamp(Date)}.
     */
    IQueueMessage<ID, DATA> qTimestamp(Date timestamp);

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
     * How many times message has been re-queued?
     * 
     * @return
     * @deprecated since v0.7.0 use {@link #getNumRequeues()}.
     */
    int qNumRequeues();

    /**
     * Set message's number of re-queue times.
     * 
     * @param numRequeues
     * @return
     * @deprecated since v0.7.0 use {@link #setNumRequeues(int)}.
     */
    IQueueMessage<ID, DATA> qNumRequeues(int numRequeues);

    /**
     * Increase message's number of re-queue times by 1.
     * 
     * @return
     * @deprecated since v0.7.0 use {$link {@link #incNumRequeues()}}.
     */
    IQueueMessage<ID, DATA> qIncNumRequeues();

    /**
     * Data/content attached to the queue message.
     * 
     * @return
     * @since 0.4.2
     * @deprecated since v0.7.0 use {@link #getData()}.
     */
    DATA qData();

    /**
     * Attach data/content to the queue message.
     * 
     * @param data
     * @return
     * @since 0.4.2
     * @deprecated since v0.7.0 use {$link {@link #setData(Object)}.
     */
    IQueueMessage<ID, DATA> qData(DATA data);

    /**
     * An empty queue message.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.3.3
     */
    @SuppressWarnings("rawtypes")
    static class EmptyQueueMessage extends EmptyMessage implements IQueueMessage {
        /**
         * {@inheritDoc}
         */
        @Override
        public EmptyQueueMessage clone() {
            return (EmptyQueueMessage) super.clone();
        }

        @Override
        public Object qId() {
            return getId();
        }

        @Override
        public EmptyQueueMessage qId(Object queueId) {
            setId(queueId);
            return this;
        }

        @Override
        public Date qOriginalTimestamp() {
            return getTimestamp();
        }

        @Override
        public EmptyQueueMessage qOriginalTimestamp(Date timestamp) {
            setTimestamp(timestamp);
            return this;
        }

        @Override
        public Date getQueueTimestamp() {
            return null;
        }

        @Override
        public EmptyQueueMessage setQueueTimestamp(Date timestamp) {
            return this;
        }

        @Override
        public Date qTimestamp() {
            return getQueueTimestamp();
        }

        @Override
        public EmptyQueueMessage qTimestamp(Date timestamp) {
            return setQueueTimestamp(timestamp);
        }

        @Override
        public int getNumRequeues() {
            return 0;
        }

        @Override
        public EmptyQueueMessage setNumRequeues(int numRequeues) {
            return this;
        }

        @Override
        public EmptyQueueMessage incNumRequeues() {
            return this;
        }

        @Override
        public int qNumRequeues() {
            return getNumRequeues();
        }

        @Override
        public EmptyQueueMessage qNumRequeues(int numRequeues) {
            return setNumRequeues(numRequeues);
        }

        @Override
        public EmptyQueueMessage qIncNumRequeues() {
            return incNumRequeues();
        }

        @Override
        public Object qData() {
            return getData();
        }

        @Override
        public EmptyQueueMessage qData(Object data) {
            setData(data);
            return this;
        }
    }
}

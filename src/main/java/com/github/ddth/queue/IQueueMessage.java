package com.github.ddth.queue;

import java.util.Date;

/**
 * Represents a queue message.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public interface IQueueMessage {
    /**
     * Message's unique id in queue.
     * 
     * @return
     */
    public Object qId();

    /**
     * Sets message's unique queue id.
     * 
     * @param id
     * @return
     */
    public IQueueMessage qId(Object queueId);

    /**
     * Message's first-queued timestamp.
     * 
     * @return
     */
    public Date qOriginalTimestamp();

    /**
     * Sets message's first-queued timestamp.
     * 
     * @param timestamp
     * @return
     */
    public IQueueMessage qOriginalTimestamp(final Date timestamp);

    /**
     * Message's last-queued timestamp.
     * 
     * @return
     */
    public Date qTimestamp();

    /**
     * Sets message's last-queued timestamp.
     * 
     * @param timestamp
     * @return
     */
    public IQueueMessage qTimestamp(final Date timestamp);

    /**
     * How many times message has been re-queued?
     * 
     * @return
     */
    public int qNumRequeues();

    /**
     * Sets message's number of re-queue times.
     * 
     * @param numRequeues
     * @return
     */
    public IQueueMessage qNumRequeues(final int numRequeues);

    /**
     * Increases message's number of re-queue times by 1.
     * 
     * @return
     */
    public IQueueMessage qIncNumRequeues();

    /**
     * An empty queue message.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.3.3
     */
    public static class EmptyQueueMessage implements IQueueMessage {

        @Override
        public Object qId() {
            return null;
        }

        @Override
        public IQueueMessage qId(Object queueId) {
            return null;
        }

        @Override
        public Date qOriginalTimestamp() {
            return null;
        }

        @Override
        public IQueueMessage qOriginalTimestamp(Date timestamp) {
            return null;
        }

        @Override
        public Date qTimestamp() {
            return null;
        }

        @Override
        public IQueueMessage qTimestamp(Date timestamp) {
            return null;
        }

        @Override
        public int qNumRequeues() {
            return 0;
        }

        @Override
        public IQueueMessage qNumRequeues(int numRequeues) {
            return null;
        }

        @Override
        public IQueueMessage qIncNumRequeues() {
            return null;
        }

    }
}

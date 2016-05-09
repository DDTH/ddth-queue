package com.github.ddth.queue;

import java.util.Collection;

/**
 * APIs to interact with queue.
 * 
 * <p>
 * Queue implementation flow:
 * <ol>
 * <li>Call {@link #take()} to get a {@link IQueueMessage} from queue.</li>
 * <li>Do something with the message.
 * <ol>
 * <li>If done with the message, call {@link #finish(IQueueMessage)}.</li>
 * <li>Otherwise, call {@link #requeue(IQueueMessage)} or
 * {@link #requeueSilent(IQueueMessage)} to requeue the message.</li>
 * </ol>
 * </li>
 * </ol>
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public interface IQueue {
    /**
     * Queues a message.
     * 
     * <p>
     * Implementation flow:
     * <ul>
     * <li>Put message to tail of queue storage.</li>
     * </ul>
     * </p>
     * 
     * @param msg
     * @return
     */
    public boolean queue(IQueueMessage msg);

    /**
     * Re-queues a message.
     * 
     * <p>
     * Implementation flow:
     * <ul>
     * <li>Put message to tail of queue storage; and increase message's re-queue
     * count & update message's queue timestamp.</li>
     * <li>Remove message from ephemeral storage.</li>
     * </ul>
     * </p>
     * 
     * <p>
     * Note: ephemeral storage implementation is optional, depends on
     * implementation.
     * </p>
     * 
     * @param msg
     * @return
     */
    public boolean requeue(IQueueMessage msg);

    /**
     * Silently re-queues a message.
     * 
     * <p>
     * Implementation flow:
     * <ul>
     * <li>Put message to tail of queue storage; do NOT increase message's
     * re-queue count and do NOT update message's queue timestamp.</li>
     * <li>Remove message from ephemeral storage.</li>
     * </ul>
     * </p>
     * 
     * <p>
     * Note: ephemeral storage implementation is optional, depends on
     * implementation.
     * </p>
     * 
     * @param msg
     * @return
     */
    public boolean requeueSilent(IQueueMessage msg);

    /**
     * Called when finish processing the message to cleanup ephemeral storage.
     * 
     * <p>
     * Implementation flow:
     * <ul>
     * <li>Remove message from ephemeral storage.</li>
     * </ul>
     * </p>
     * 
     * <p>
     * Note: ephemeral storage implementation is optional, depends on
     * implementation.
     * </p>
     * 
     * @param msg
     */
    public void finish(IQueueMessage msg);

    /**
     * Takes a message out of queue.
     * 
     * <p>
     * Implementation flow:
     * <ul>
     * <li>Read message from head of queue storage.</li>
     * <li>Write message to ephemeral storage.</li>
     * <li>Remove message from queue storage.</li>
     * </ul>
     * </p>
     * 
     * <p>
     * Note: ephemeral storage implementation is optional, depends on
     * implementation.
     * </p>
     * 
     * @return
     */
    public IQueueMessage take();

    /**
     * Gets all orphan messages (messages that were left in ephemeral storage
     * for a long time).
     * 
     * @param thresholdTimestampMs
     *            get all orphan messages that were queued
     *            <strong>before</strong> this timestamp
     * @return
     * @since 0.2.0
     */
    public Collection<IQueueMessage> getOrphanMessages(long thresholdTimestampMs);

    /**
     * Moves a message from ephemeral back to queue storage. Useful when dealing
     * with orphan messages.
     * 
     * <p>
     * Implementation flow:
     * <ul>
     * <li>Read message from the ephemeral storage.</li>
     * <li>Put the message back to queue.</li>
     * <li>Remove the message from the ephemeral storage.</li>
     * </ul>
     * </p>
     * 
     * @param msg
     * @return {@code true} if a move has been made, {@code false} otherwise
     *         (e.g. the message didn't exist in ephemeral storage)
     * @since 0.2.1
     */
    public boolean moveFromEphemeralToQueueStorage(IQueueMessage msg);

    /**
     * Gets queue's number of items.
     * 
     * @return
     */
    public int queueSize();

    /**
     * Gets ephemeral-storage's number of items.
     * 
     * <p>
     * Note: ephemeral storage implementation is optional, depends on
     * implementation.
     * </p>
     * 
     * @return
     */
    public int ephemeralSize();
}

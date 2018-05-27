package com.github.ddth.queue;

import java.util.Collection;

import com.github.ddth.queue.utils.QueueException;

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
public interface IQueue<ID, DATA> {

    /**
     * Return this value to indicate that get-size functionality is not
     * supported.
     * 
     * @since 0.7.1
     */
    static int SIZE_NOT_SUPPORTED = -1;

    /**
     * Create a new, empty queue message.
     * 
     * @return
     * @since 0.6.0
     */
    IQueueMessage<ID, DATA> createMessage();

    /**
     * Create a new queue message with supplied content.
     * 
     * @param content
     * @return
     * @since 0.6.0
     */
    IQueueMessage<ID, DATA> createMessage(DATA content);

    /**
     * Create a new queue message with supplied id and content.
     * 
     * @param id
     * @param content
     * @return
     * @since 0.6.0
     */
    IQueueMessage<ID, DATA> createMessage(ID id, DATA content);

    /**
     * Set queue's event observer.
     * 
     * @param observer
     * @return
     */
    IQueue<ID, DATA> setObserver(IQueueObserver<ID, DATA> observer);

    /**
     * Queue a message.
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
     * @throws QueueException.QueueIsFull
     *             if queue storage is full, can not take any more message
     * @throws QueueException.CannotSerializeQueueMessage
     *             if the supplied message can not be serialize
     * @throws QueueException
     *             other queue exception
     */
    boolean queue(IQueueMessage<ID, DATA> msg) throws QueueException;

    /**
     * Re-queue a message.
     * 
     * <p>
     * Implementation flow:
     * <ul>
     * <li>Put message to the queue storage (head or tail position depends on
     * the queue implementation); and increase message's re-queue count & update
     * message's queue timestamp.</li>
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
     * @throws QueueException.QueueIsFull
     *             if queue storage is full, can not take any more message
     * @throws QueueException.CannotSerializeQueueMessage
     *             if the supplied message can not be serialize
     * @throws QueueException
     *             other queue exception
     */
    boolean requeue(IQueueMessage<ID, DATA> msg) throws QueueException;

    /**
     * Silently re-queue a message.
     * 
     * <p>
     * Implementation flow:
     * <ul>
     * <li>Put message to the queue storage (head or tail position depends on
     * the queue implementation); do NOT increase message's re-queue count and
     * do NOT update message's queue timestamp.</li>
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
     * @throws QueueException.QueueIsFull
     *             if queue storage is full, can not take any more message
     * @throws QueueException.CannotSerializeQueueMessage
     *             if the supplied message can not be serialize
     * @throws QueueException
     *             other queue exception
     */
    boolean requeueSilent(IQueueMessage<ID, DATA> msg) throws QueueException;

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
     * @throws QueueException
     */
    void finish(IQueueMessage<ID, DATA> msg) throws QueueException;

    /**
     * Take a message out of queue.
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
     * @throws QueueException.EphemeralIsFull
     *             if ephemeral storage is full, can not put message to
     *             ephemeral storage
     * @throws QueueException.CannotDeserializeQueueMessage
     *             if the queue message can not be deserialized
     * @throws QueueException
     *             other queue exception
     */
    IQueueMessage<ID, DATA> take() throws QueueException;

    /**
     * Get all orphan messages (messages that were left in ephemeral storage for
     * a long time).
     * 
     * @param thresholdTimestampMs
     *            message is orphan if
     *            {@code message's timestampMillis + thresholdTimestampMs < now}
     *            . Which means {@code getOrphanMessages(10000)} will return
     *            orphan messages that have stayed in ephemeral storage for more
     *            than 10000 milliseconds.
     * @return {@code null} or empty collection if there is no orphan message
     * @since 0.2.0
     * @throws QueueException.OperationNotSupported
     */
    Collection<IQueueMessage<ID, DATA>> getOrphanMessages(long thresholdTimestampMs)
            throws QueueException.OperationNotSupported;

    /**
     * Move a message from ephemeral back to queue storage. Useful when dealing
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
     * <p>
     * Note: implementation should not throw {@link QueueException.QueueIsFull}
     * exception.
     * </p>
     * 
     * @param msg
     * @return {@code true} if a move has been made, {@code false} otherwise
     *         (e.g. the message didn't exist in ephemeral storage)
     * @since 0.2.1
     * @throws QueueException.OperationNotSupported
     * @deprecated since v0.7.1 use {@link #requeue(IQueueMessage)} or
     *             {@link #requeueSilent(IQueueMessage)}
     */
    @Deprecated
    boolean moveFromEphemeralToQueueStorage(IQueueMessage<ID, DATA> msg)
            throws QueueException.OperationNotSupported;

    /**
     * Get number of items currently in queue storage.
     * 
     * @return negative number if queue size can not be queried
     * @throws QueueException
     */
    int queueSize() throws QueueException;

    /**
     * Get number of items currently in ephemeral storage.
     * 
     * <p>
     * Note: ephemeral storage implementation is optional, depends on
     * implementation.
     * </p>
     * 
     * @return negative number if ephemeral size can not be queried
     * @throws QueueException
     */
    int ephemeralSize() throws QueueException;
}

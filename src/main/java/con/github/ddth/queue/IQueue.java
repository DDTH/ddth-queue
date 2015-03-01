package con.github.ddth.queue;

/**
 * APIs to interact with queue.
 * 
 * <p>
 * Queue implementation flow:
 * <ol>
 * <li>{@link #take()} message from queue.</li>
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
     * & update message's queue timestamp. count.</li>
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
     * re-queue count or update message's queue timestamp.</li>
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
     * Gets queue's number of items.
     * 
     * @return
     */
    public int queueSize();

    /**
     * Gets ephemeral-queue's number of items.
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

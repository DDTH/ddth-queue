package com.github.ddth.queue;

/**
 * Interface to observe queue's events.
 * 
 * @author Thanh Nguyen <btnguyen2@gmail.com>
 * @since 0.6.0
 */
public interface IQueueObserver<ID, DATA> {
    /**
     * Called before queue is initialized.
     * 
     * @param queue
     */
    void preInit(IQueue<ID, DATA> queue);

    /**
     * Called after queue is initialized.
     * 
     * @param queue
     */
    void postInit(IQueue<ID, DATA> queue);

    /**
     * Called before queue is destroyed.
     * 
     * @param queue
     */
    void preDestroy(IQueue<ID, DATA> queue);

    /**
     * Called after queue is destroyed.
     * 
     * @param queue
     */
    void postDestroy(IQueue<ID, DATA> queue);

}

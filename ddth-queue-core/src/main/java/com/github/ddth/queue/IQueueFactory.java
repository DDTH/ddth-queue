package com.github.ddth.queue;

/**
 * Factory to create {@link IQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public interface IQueueFactory<ID, DATA> {
    /**
     * Gets an {@link IQueue} instance.
     * 
     * @param spec
     *            concrete class defines format of {@code spec}.
     * @return
     */
    IQueue<ID, DATA> getQueue(QueueSpec spec);
}

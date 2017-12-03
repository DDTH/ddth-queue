package com.github.ddth.queue;

/**
 * No-op implementation of {@link IQueueObserver}.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 *
 * @param <ID>
 * @param <DATA>
 * @since 0.6.0
 */
public class NoopQueueObserver<ID, DATA> implements IQueueObserver<ID, DATA> {
    @Override
    public void preInit(IQueue<ID, DATA> queue) {
        // EMPTY
    }

    @Override
    public void postInit(IQueue<ID, DATA> queue) {
        // EMPTY
    }

    @Override
    public void preDestroy(IQueue<ID, DATA> queue) {
        // EMPTY
    }

    @Override
    public void postDestroy(IQueue<ID, DATA> queue) {
        // EMPTY
    }
}
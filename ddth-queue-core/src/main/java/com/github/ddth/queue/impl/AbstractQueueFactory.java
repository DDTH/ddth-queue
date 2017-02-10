package com.github.ddth.queue.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.github.ddth.queue.IQueueFactory;
import com.github.ddth.queue.QueueSpec;

public abstract class AbstractQueueFactory<T extends AbstractQueue> implements IQueueFactory {

    protected ConcurrentMap<QueueSpec, T> queueInstances = new ConcurrentHashMap<QueueSpec, T>();

    public AbstractQueueFactory<T> init() {
        return this;
    }

    public void destroy() {
        queueInstances.clear();
    }

    /**
     * Creates a new queue instance.
     * 
     * <p>
     * Called by {@link #createAndInitQueue(QueueSpec)}. Sub-class is to
     * implement this method.
     * </p>
     * 
     * @param spec
     * @return
     */
    protected abstract T createQueueInstance(QueueSpec spec);

    /**
     * Initializes a newly created queue instance.
     * 
     * <p>
     * Called by {@link #createAndInitQueue(QueueSpec)}. Sub-class may override
     * this method to implement its own business logic.
     * </p>
     * 
     * @param queue
     * @param spec
     */
    protected void initQueue(T queue, QueueSpec spec) {
        // EMPTY
    }

    /**
     * Creates & Initializes a new queue instance.
     * 
     * @param spec
     * @return
     */
    protected T createAndInitQueue(QueueSpec spec) {
        T queue = createQueueInstance(spec);
        initQueue(queue, spec);
        return queue;
    }

    protected void disposeQueue(T queue) {
        queue.destroy();
    }

    protected void disposeQueue(QueueSpec id, T queue) {
        queueInstances.remove(id, queue);
        disposeQueue(queue);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getQueue(QueueSpec spec) {
        T queue = queueInstances.get(spec);
        if (queue == null) {
            queue = createAndInitQueue(spec);
            if (queue != null) {
                T existingQueue = queueInstances.putIfAbsent(spec, queue);
                if (existingQueue != null) {
                    disposeQueue(queue);
                    queue = existingQueue;
                }
            }
        }
        return queue;
    }

}

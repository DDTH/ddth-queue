package com.github.ddth.queue.impl;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import com.github.ddth.queue.IQueueFactory;
import com.github.ddth.queue.IQueueObserver;
import com.github.ddth.queue.QueueSpec;

public abstract class AbstractQueueFactory<T extends AbstractQueue<ID, DATA>, ID, DATA>
        implements IQueueFactory<ID, DATA> {

    private ConcurrentMap<QueueSpec, T> queueInstances = new ConcurrentHashMap<>();
    private IQueueObserver<ID, DATA> defaultObserver;

    private boolean defaultEphemeralDisabled = false;
    private int defaultMaxSize = QueueSpec.NO_BOUNDARY,
            defaultEphemeralMaxSize = QueueSpec.NO_BOUNDARY;

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public boolean isDefaultEphemeralDisabled() {
        return defaultEphemeralDisabled;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public boolean getDefaultEphemeralDisabled() {
        return defaultEphemeralDisabled;
    }

    /**
     * 
     * @param defaultEphemeralDisabled
     * @since 0.6.2
     */
    public void setDefaultEphemeralDisabled(boolean defaultEphemeralDisabled) {
        this.defaultEphemeralDisabled = defaultEphemeralDisabled;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public int getDefaultMaxSize() {
        return defaultMaxSize;
    }

    /**
     * 
     * @param defaultMaxSize
     * @since 0.6.2
     */
    public void setDefaultMaxSize(int defaultMaxSize) {
        this.defaultMaxSize = defaultMaxSize;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public int getDefaultEphemeralMaxSize() {
        return defaultEphemeralMaxSize;
    }

    /**
     * 
     * @param defaultEphemeralMaxSize
     * @since 0.6.2
     */
    public void setDefaultEphemeralMaxSize(int defaultEphemeralMaxSize) {
        this.defaultEphemeralMaxSize = defaultEphemeralMaxSize;
    }

    /**
     * Get default queue's event observer.
     * 
     * @return
     * @since 0.6.0
     */
    public IQueueObserver<ID, DATA> getDefaultObserver() {
        return defaultObserver;
    }

    /**
     * Set default queue's event observer.
     * 
     * @param defaultObserver
     * @return
     * @since 0.6.0
     */
    public AbstractQueueFactory<T, ID, DATA> setDefaultObserver(
            IQueueObserver<ID, DATA> defaultObserver) {
        this.defaultObserver = defaultObserver;
        return this;
    }

    public AbstractQueueFactory<T, ID, DATA> init() {
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
        queue.setObserver(defaultObserver);
    }

    /**
     * Creates & Initializes a new queue instance.
     * 
     * @param spec
     * @return
     */
    protected T createAndInitQueue(QueueSpec spec) {
        T queue = createQueueInstance(spec);
        queue.setQueueName(spec.name);
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

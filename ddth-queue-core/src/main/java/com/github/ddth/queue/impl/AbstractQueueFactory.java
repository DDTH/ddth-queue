package com.github.ddth.queue.impl;

import com.github.ddth.queue.IQueueFactory;
import com.github.ddth.queue.QueueSpec;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.RemovalListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

/**
 * Abstract queue factory implementation.
 *
 * @param <T>
 * @param <ID>
 * @param <DATA>
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 */
public abstract class AbstractQueueFactory<T extends AbstractQueue<ID, DATA>, ID, DATA>
        implements IQueueFactory<ID, DATA>, AutoCloseable {

    private final Logger LOGGER = LoggerFactory.getLogger(AbstractQueueFactory.class);

    private Cache<QueueSpec, T> queueInstances = CacheBuilder.newBuilder().expireAfterAccess(3600, TimeUnit.SECONDS)
            .removalListener((RemovalListener<QueueSpec, T>) notification -> {
                T queue = notification.getValue();
                try {
                    queue.destroy();
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                }
            }).build();
    //private IQueueObserver<ID, DATA> defaultObserver;

    private boolean defaultEphemeralDisabled = false;
    private int defaultMaxSize = QueueSpec.NO_BOUNDARY, defaultEphemeralMaxSize = QueueSpec.NO_BOUNDARY;

    /**
     * @return
     * @since 0.6.2
     */
    public boolean isDefaultEphemeralDisabled() {
        return defaultEphemeralDisabled;
    }

    /**
     * @return
     * @since 0.6.2
     */
    public boolean getDefaultEphemeralDisabled() {
        return defaultEphemeralDisabled;
    }

    /**
     * @param defaultEphemeralDisabled
     * @since 0.6.2
     */
    public AbstractQueueFactory<T, ID, DATA> setDefaultEphemeralDisabled(boolean defaultEphemeralDisabled) {
        this.defaultEphemeralDisabled = defaultEphemeralDisabled;
        return this;
    }

    /**
     * @return
     * @since 0.6.2
     */
    public int getDefaultMaxSize() {
        return defaultMaxSize;
    }

    /**
     * @param defaultMaxSize
     * @since 0.6.2
     */
    public AbstractQueueFactory<T, ID, DATA> setDefaultMaxSize(int defaultMaxSize) {
        this.defaultMaxSize = defaultMaxSize;
        return this;
    }

    /**
     * @return
     * @since 0.6.2
     */
    public int getDefaultEphemeralMaxSize() {
        return defaultEphemeralMaxSize;
    }

    /**
     * @param defaultEphemeralMaxSize
     * @since 0.6.2
     */
    public AbstractQueueFactory<T, ID, DATA> setDefaultEphemeralMaxSize(int defaultEphemeralMaxSize) {
        this.defaultEphemeralMaxSize = defaultEphemeralMaxSize;
        return this;
    }

    //    /**
    //     * Get default queue's event observer.
    //     *
    //     * @return
    //     * @since 0.6.0
    //     */
    //    public IQueueObserver<ID, DATA> getDefaultObserver() {
    //        return defaultObserver;
    //    }
    //
    //    /**
    //     * Set default queue's event observer.
    //     *
    //     * @param defaultObserver
    //     * @return
    //     * @since 0.6.0
    //     */
    //    public AbstractQueueFactory<T, ID, DATA> setDefaultObserver(IQueueObserver<ID, DATA> defaultObserver) {
    //        this.defaultObserver = defaultObserver;
    //        return this;
    //    }

    public AbstractQueueFactory<T, ID, DATA> init() {
        return this;
    }

    public void destroy() {
        queueInstances.invalidateAll();
    }

    public void close() {
        destroy();
    }

    /**
     * Create a new queue instance.
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
     * Initialize a newly created queue instance.
     *
     * <p>
     * Called by {@link #createAndInitQueue(QueueSpec)}. Sub-class may override
     * this method to implement its own business logic.
     * </p>
     *
     * @param queue
     * @param spec
     */
    protected void initQueue(T queue, QueueSpec spec) throws Exception {
        //        if (queue.getObserver() == null) {
        //            queue.setObserver(defaultObserver);
        //        }
        queue.init();
    }

    /**
     * Create & initialize a new queue instance.
     *
     * @param spec
     * @return
     * @throws Exception
     */
    protected T createAndInitQueue(QueueSpec spec) throws Exception {
        T queue = createQueueInstance(spec);
        queue.setQueueName(spec.name);
        initQueue(queue, spec);
        return queue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public T getQueue(QueueSpec spec) {
        try {
            T queue = queueInstances.get(spec, () -> createAndInitQueue(spec));
            return queue;
        } catch (ExecutionException e) {
            throw new RuntimeException(e.getCause());
        }
    }
}

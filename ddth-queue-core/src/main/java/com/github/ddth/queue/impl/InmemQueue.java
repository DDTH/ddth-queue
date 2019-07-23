package com.github.ddth.queue.impl;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.utils.QueueException;

import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * In-Memory implementation of {@link IQueue}.
 *
 * <p>
 * Implementation:
 * <ul>
 * <li>A {@link Queue} as queue storage.</li>
 * <li>A {@link ConcurrentMap} as ephemeral storage.</li>
 * </ul>
 * </p>
 *
 * <p>Features:</p>
 * <ul>
 * <li>Queue-size support: yes</li>
 * <li>Ephemeral storage support: yes</li>
 * <li>Ephemeral-size support: yes</li>
 * </ul>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class InmemQueue<ID, DATA> extends AbstractInmemEphemeralQueue<ID, DATA> {
    private Queue<IQueueMessage<ID, DATA>> queue;

    /**
     * A value less than {@code 1} mean "no boundary".
     */
    private int boundary = QueueSpec.NO_BOUNDARY;

    public InmemQueue() {
    }

    public InmemQueue(int boundary) {
        setBoundary(boundary);
    }

    /**
     * Gets queue's boundary (max number of elements).
     *
     * @return
     */
    public int getBoundary() {
        return boundary;
    }

    /**
     * Sets queue's boundary (max number of elements).
     *
     * @param boundary queue's max number of elements, a value less than {@code 1}
     *                 mean "no boundary".
     * @return
     */
    public InmemQueue<ID, DATA> setBoundary(int boundary) {
        this.boundary = boundary;
        return this;
    }

    /**
     * This method will create a {@link Queue} instance with the following
     * rules:
     *
     * <ul>
     * <li>If {@link #boundary} is set and larger than {@code 1024}, a
     * {@link LinkedBlockingQueue} is created; if {@link #boundary} is less than
     * or equals to {@code 1024}, an {@link ArrayBlockingQueue} is created
     * instead.</li>
     * <li>Otherwise, a {@link ConcurrentLinkedQueue} is created.</li>
     * </ul>
     *
     * @param boundary
     * @return
     */
    protected Queue<IQueueMessage<ID, DATA>> createQueue(int boundary) {
        if (boundary > 0) {
            if (boundary > 1024) {
                return new LinkedBlockingQueue<>(boundary);
            } else {
                return new ArrayBlockingQueue<>(boundary);
            }
        } else {
            return new ConcurrentLinkedQueue<>();
        }
    }

    /**
     * Init method.
     *
     * @return
     * @throws Exception
     */
    public InmemQueue<ID, DATA> init() throws Exception {
        queue = createQueue(boundary);
        initEphemeralStorage(1024);
        super.init();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean doPutToQueue(IQueueMessage<ID, DATA> msg, PutToQueueCase queueCase)
            throws QueueException.QueueIsFull {
        if (!queue.offer(msg)) {
            throw new QueueException.QueueIsFull(getBoundary());
        }
        if (queueCase != null && queueCase != PutToQueueCase.NEW) {
            doRemoveFromEphemeralStorage(msg);
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage<ID, DATA> msg) {
        doRemoveFromEphemeralStorage(msg);
    }

    /**
     * Takes a message from the internal queue.
     *
     * @return
     */
    protected IQueueMessage<ID, DATA> takeFromQueue() {
        return queue.poll();
    }

    /**
     * {@inheritDoc}
     *
     * @throws QueueException.EphemeralIsFull if the ephemeral storage is full
     */
    @Override
    public IQueueMessage<ID, DATA> take() throws QueueException.EphemeralIsFull {
        ensureEphemeralSize();
        IQueueMessage<ID, DATA> msg = takeFromQueue();
        doPutToEphemeralStorage(msg);
        return msg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        return queue.size();
    }
}

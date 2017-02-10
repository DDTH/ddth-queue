package com.github.ddth.queue.impl;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.Queue;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.LinkedBlockingQueue;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.utils.QueueException;

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
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.0
 */
public class InmemQueue extends AbstractEphemeralSupportQueue {

    private Queue<IQueueMessage> queue;
    private ConcurrentMap<Object, IQueueMessage> ephemeralStorage;

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
     * @param boundary
     *            queue's max number of elements, a value less than {@code 1}
     *            mean "no boundary".
     * @return
     */
    public InmemQueue setBoundary(int boundary) {
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
    protected Queue<IQueueMessage> createQueue(int boundary) {
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
     */
    public InmemQueue init() {
        queue = createQueue(boundary);
        if (!isEphemeralDisabled()) {
            ephemeralStorage = new ConcurrentHashMap<>();
        }
        return this;
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        // EMPTY
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        destroy();
    }

    /**
     * Puts a message to the queue buffer.
     * 
     * @param msg
     * @throws QueueException.QueueIsFull
     *             if the ring buffer is full
     * 
     */
    protected void putToQueue(IQueueMessage msg) throws QueueException.QueueIsFull {
        if (!queue.offer(msg)) {
            throw new QueueException.QueueIsFull(getBoundary());
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @throws QueueException.QueueIsFull
     *             if queue buffer is full
     */
    @Override
    public boolean queue(IQueueMessage _msg) throws QueueException.QueueIsFull {
        IQueueMessage msg = _msg.clone();
        Date now = new Date();
        msg.qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now);
        putToQueue(msg);
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @throws QueueException.QueueIsFull
     *             if queue buffer is full
     */
    @Override
    public boolean requeue(IQueueMessage _msg) throws QueueException.QueueIsFull {
        IQueueMessage msg = _msg.clone();
        Date now = new Date();
        msg.qIncNumRequeues().qTimestamp(now);
        putToQueue(msg);
        if (!isEphemeralDisabled()) {
            ephemeralStorage.remove(msg.qId());
        }
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @throws QueueException.QueueIsFull
     *             if queue buffer is full
     */
    @Override
    public boolean requeueSilent(IQueueMessage _msg) throws QueueException.QueueIsFull {
        IQueueMessage msg = _msg.clone();
        putToQueue(msg);
        if (!isEphemeralDisabled()) {
            ephemeralStorage.remove(msg.qId());
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage msg) {
        if (!isEphemeralDisabled()) {
            ephemeralStorage.remove(msg.qId());
        }
    }

    /**
     * Takes a message from the internal queue.
     * 
     * @return
     */
    protected IQueueMessage takeFromQueue() {
        return queue.poll();
    }

    /**
     * {@inheritDoc}
     * 
     * @throws QueueException.EphemeralIsFull
     *             if the ephemeral storage is full
     */
    @Override
    public IQueueMessage take() throws QueueException.EphemeralIsFull {
        if (!isEphemeralDisabled()) {
            int ephemeralMaxSize = getEphemeralMaxSize();
            if (ephemeralMaxSize > 0 && ephemeralStorage.size() >= ephemeralMaxSize) {
                throw new QueueException.EphemeralIsFull(ephemeralMaxSize);
            }
        }
        IQueueMessage msg = takeFromQueue();
        if (msg != null && !isEphemeralDisabled()) {
            ephemeralStorage.putIfAbsent(msg.qId(), msg);
        }
        return msg;
    }

    /**
     * {@inheritDoc}
     * 
     * @param thresholdTimestampMs
     * @return
     */
    @Override
    public Collection<IQueueMessage> getOrphanMessages(long thresholdTimestampMs) {
        if (isEphemeralDisabled()) {
            return null;
        }
        Collection<IQueueMessage> orphanMessages = new HashSet<>();
        long now = System.currentTimeMillis();
        for (Entry<?, IQueueMessage> entry : ephemeralStorage.entrySet()) {
            IQueueMessage msg = entry.getValue();
            if (msg.qOriginalTimestamp().getTime() + thresholdTimestampMs < now) {
                orphanMessages.add(msg);
            }
        }
        return orphanMessages;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean moveFromEphemeralToQueueStorage(IQueueMessage _msg) {
        if (!isEphemeralDisabled()) {
            IQueueMessage msg = ephemeralStorage.remove(_msg.qId());
            if (msg != null) {
                try {
                    putToQueue(msg);
                    return true;
                } catch (QueueException.QueueIsFull e) {
                    ephemeralStorage.putIfAbsent(msg.qId(), msg);
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        return queue.size();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        return !isEphemeralDisabled() ? ephemeralStorage.size() : 0;
    }
}

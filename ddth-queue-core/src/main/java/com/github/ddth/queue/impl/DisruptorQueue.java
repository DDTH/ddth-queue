package com.github.ddth.queue.impl;

import java.io.Closeable;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

/**
 * In-Memory implementation of {@link IQueue} using LMAX Disruptor library.
 * 
 * <p>
 * Implementation:
 * <ul>
 * <li>A Disruptor's {@link RingBuffer} to be the queue storage.</li>
 * <li>A {@link ConcurrentMap} as ephemeral storage.</li>
 * </ul>
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.0
 * @see https://lmax-exchange.github.io/disruptor/
 */
public class DisruptorQueue implements IQueue, Closeable, AutoCloseable {

    private final static class Event {
        private IQueueMessage value;

        public void set(IQueueMessage value) {
            this.value = value;
        }

        public IQueueMessage get() {
            return value;
        }
    }

    private final static EventFactory<Event> EVENT_FACTORY = new EventFactory<Event>() {
        @Override
        public Event newInstance() {
            return new Event();
        }
    };

    private ConcurrentMap<Object, IQueueMessage> ephemeralStorage;

    private boolean ephemeralDisabled = false;
    private RingBuffer<Event> ringBuffer;
    private Sequence consumedSeq;
    private long knownPublishedSeq;
    private int ringSize = 1024;

    public DisruptorQueue() {
    }

    public DisruptorQueue(int ringSize) {
        setRingSize(ringSize);
    }

    /**
     * Is ephemeral storage disabled?
     * 
     * @return
     */
    public boolean getEphemeralDisabled() {
        return ephemeralDisabled;
    }

    /**
     * Is ephemeral storage disabled?
     * 
     * @return
     */
    public boolean isEphemeralDisabled() {
        return ephemeralDisabled;
    }

    /**
     * Disables/Enables ephemeral storage.
     * 
     * @param ephemeralDisabled
     *            {@code true} to disable ephemeral storage, {@code false}
     *            otherwise.
     * @return
     */
    public DisruptorQueue setEphemeralDisabled(boolean ephemeralDisabled) {
        this.ephemeralDisabled = ephemeralDisabled;
        return this;
    }

    /**
     * Gets size of the ring buffer.
     * 
     * @return
     */
    protected int getRingSize() {
        return ringSize;
    }

    private static int nextPowerOf2(int n) {
        if (n < 2) {
            return 2;
        }

        /*
         * bithack:
         * http://graphics.stanford.edu/~seander/bithacks.html#RoundUpPowerOf2
         */
        n--;
        n |= n >> 1;
        n |= n >> 2;
        n |= n >> 4;
        n |= n >> 8;
        n |= n >> 16;
        n++;
        return n;
    }

    /**
     * Sets size of the ring buffer, must be power of 2.
     * 
     * @param ringSize
     * @return
     */
    public DisruptorQueue setRingSize(int ringSize) {
        this.ringSize = nextPowerOf2(ringSize);
        return this;
    }

    /**
     * Init method.
     * 
     * @return
     */
    public DisruptorQueue init() {
        ringBuffer = RingBuffer.createSingleProducer(EVENT_FACTORY, ringSize);
        // ringBuffer = RingBuffer.createMultiProducer(EVENT_FACTORY, ringSize);

        if (!ephemeralDisabled) {
            ephemeralStorage = new ConcurrentHashMap<>(ringSize);
        }

        consumedSeq = new Sequence();
        ringBuffer.addGatingSequences(consumedSeq);
        // SequenceBarrier barrier = ringBuffer.newBarrier();
        long cursor = ringBuffer.getCursor();
        consumedSeq.set(cursor);
        knownPublishedSeq = cursor;

        return this;
    }

    /**
     * Destroy method.
     */
    public void destroy() {
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        destroy();
    }

    /**
     * Publishes (commits) the ring's sequence.
     * 
     * @param value
     * @param seq
     */
    protected void publish(IQueueMessage value, long seq) {
        Event holder = ringBuffer.get(seq);
        holder.set(value);
        ringBuffer.publish(seq);
    }

    /**
     * Puts a message to the ring buffer.
     * 
     * @param msg
     * @return {@code true} if successful, {@code false} otherwise (the ring
     *         buffer is full
     */
    protected boolean putToRingBuffer(IQueueMessage msg) {
        LOCK_PUT.lock();
        try {
            long seq;
            try {
                seq = ringBuffer.tryNext();
            } catch (InsufficientCapacityException e1) {
                return false;
            }
            publish(msg, seq);
            return true;
        } finally {
            LOCK_PUT.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean queue(IQueueMessage _msg) {
        IQueueMessage msg = _msg.clone();
        Date now = new Date();
        msg.qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now);
        return putToRingBuffer(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeue(IQueueMessage _msg) {
        IQueueMessage msg = _msg.clone();
        Date now = new Date();
        msg.qIncNumRequeues().qTimestamp(now);
        if (putToRingBuffer(msg)) {
            if (!ephemeralDisabled) {
                ephemeralStorage.remove(msg.qId());
            }
            return true;
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeueSilent(IQueueMessage msg) {
        if (putToRingBuffer(msg)) {
            if (!ephemeralDisabled) {
                ephemeralStorage.remove(msg.qId());
            }
            return true;
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage msg) {
        if (!ephemeralDisabled) {
            ephemeralStorage.remove(msg.qId());
        }
    }

    /**
     * Updates the published sequence number after a message has been
     * successfully taken from queue.
     */
    protected void updatePublishedSequence() {
        long c = ringBuffer.getCursor();
        if (c >= knownPublishedSeq + 1) {
            long pos = c;
            for (long sequence = knownPublishedSeq + 1; sequence <= c; sequence++) {
                if (!ringBuffer.isPublished(sequence)) {
                    pos = sequence - 1;
                    break;
                }
            }
            knownPublishedSeq = pos;
        }
    }

    private Lock LOCK_TAKE = new ReentrantLock();
    private Lock LOCK_PUT = new ReentrantLock();

    /**
     * Takes a message from the ring buffer.
     * 
     * @return the available message or {@code null} if the ring buffer is empty
     */
    protected IQueueMessage takeFromRingBuffer() {
        LOCK_TAKE.lock();
        try {
            long l = consumedSeq.get() + 1;
            if (l > knownPublishedSeq) {
                updatePublishedSequence();
            }
            if (l <= knownPublishedSeq) {
                Event eventHolder = ringBuffer.get(l);
                IQueueMessage value = eventHolder.get();
                consumedSeq.incrementAndGet();
                return value;
            }
            return null;
        } finally {
            LOCK_TAKE.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IQueueMessage take() {
        IQueueMessage msg = takeFromRingBuffer();
        if (msg != null && !ephemeralDisabled) {
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
        if (ephemeralDisabled) {
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
        if (!ephemeralDisabled) {
            IQueueMessage msg = ephemeralStorage.remove(_msg.qId());
            if (msg != null) {
                if (putToRingBuffer(msg)) {
                    return true;
                } else {
                    ephemeralStorage.putIfAbsent(msg.qId(), msg);
                }
            }
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        return (int) (ringBuffer.getCursor() - consumedSeq.get());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        return ephemeralDisabled ? -1 : ephemeralStorage.size();
    }
}

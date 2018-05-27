package com.github.ddth.queue.impl;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;
import com.lmax.disruptor.EventFactory;
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
public class DisruptorQueue<ID, DATA> extends AbstractEphemeralSupportQueue<ID, DATA> {

    private final static class Event<ID, DATA> {
        private IQueueMessage<ID, DATA> value;

        public void set(IQueueMessage<ID, DATA> value) {
            this.value = value;
        }

        public IQueueMessage<ID, DATA> get() {
            return value;
        }
    }

    private final EventFactory<Event<ID, DATA>> EVENT_FACTORY = () -> new Event<ID, DATA>();
    private final Lock LOCK_TAKE = new ReentrantLock();
    private final Lock LOCK_PUT = new ReentrantLock();
    private ConcurrentMap<Object, IQueueMessage<ID, DATA>> ephemeralStorage;

    private RingBuffer<Event<ID, DATA>> ringBuffer;
    private Sequence consumedSeq;
    private long knownPublishedSeq;
    private int ringSize = 1024;

    public DisruptorQueue() {
    }

    public DisruptorQueue(int ringSize) {
        setRingSize(ringSize);
    }

    /**
     * Get size of the ring buffer.
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
     * Set size of the ring buffer, must be power of 2.
     * 
     * @param ringSize
     * @return
     */
    public DisruptorQueue<ID, DATA> setRingSize(int ringSize) {
        this.ringSize = nextPowerOf2(ringSize);
        return this;
    }

    /**
     * Init method.
     * 
     * @return
     * @throws Exception
     */
    public DisruptorQueue<ID, DATA> init() throws Exception {
        /* single producer "seems" to offer better performance */
        ringBuffer = RingBuffer.createSingleProducer(EVENT_FACTORY, ringSize);
        // ringBuffer = RingBuffer.createMultiProducer(EVENT_FACTORY, ringSize);

        if (!isEphemeralDisabled()) {
            int ephemeralBoundSize = Math.max(0, getEphemeralMaxSize());
            ephemeralStorage = new ConcurrentHashMap<>(
                    ephemeralBoundSize > 0 ? Math.min(ephemeralBoundSize, ringSize) : ringSize);
        }

        consumedSeq = new Sequence();
        ringBuffer.addGatingSequences(consumedSeq);
        long cursor = ringBuffer.getCursor();
        consumedSeq.set(cursor);
        knownPublishedSeq = cursor;

        super.init();

        return this;
    }

    /**
     * Put a message to the ring buffer.
     * 
     * @param msg
     * @throws QueueException.QueueIsFull
     *             if the ring buffer is full
     */
    protected void putToRingBuffer(IQueueMessage<ID, DATA> msg) throws QueueException.QueueIsFull {
        if (msg == null) {
            throw new NullPointerException("Supplied queue message is null!");
        }
        LOCK_PUT.lock();
        try {
            if (!ringBuffer.tryPublishEvent((event, _seq) -> {
                event.set(msg);
                knownPublishedSeq = _seq > knownPublishedSeq ? _seq : knownPublishedSeq;
            })) {
                throw new QueueException.QueueIsFull(getRingSize());
            }
        } finally {
            LOCK_PUT.unlock();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @throws QueueException.QueueIsFull
     *             if the ring buffer is full
     */
    @Override
    public boolean queue(IQueueMessage<ID, DATA> _msg) throws QueueException.QueueIsFull {
        IQueueMessage<ID, DATA> msg = _msg.clone();
        Date now = new Date();
        msg.setNumRequeues(0).setQueueTimestamp(now).setTimestamp(now);
        putToRingBuffer(msg);
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @throws QueueException.QueueIsFull
     *             if the ring buffer is full
     */
    @Override
    public boolean requeue(IQueueMessage<ID, DATA> _msg) throws QueueException.QueueIsFull {
        IQueueMessage<ID, DATA> msg = _msg.clone();
        Date now = new Date();
        msg.incNumRequeues().setQueueTimestamp(now);
        putToRingBuffer(msg);
        if (!isEphemeralDisabled()) {
            ephemeralStorage.remove(msg.getId());
        }
        return true;
    }

    /**
     * {@inheritDoc}
     * 
     * @throws QueueException.QueueIsFull
     *             if the ring buffer is full
     */
    @Override
    public boolean requeueSilent(IQueueMessage<ID, DATA> _msg) throws QueueException.QueueIsFull {
        IQueueMessage<ID, DATA> msg = _msg.clone();
        putToRingBuffer(msg);
        if (!isEphemeralDisabled()) {
            ephemeralStorage.remove(msg.getId());
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage<ID, DATA> msg) {
        if (!isEphemeralDisabled()) {
            ephemeralStorage.remove(msg.getId());
        }
    }

    /**
     * Takes a message from the ring buffer.
     * 
     * @return the available message or {@code null} if the ring buffer is empty
     */
    protected IQueueMessage<ID, DATA> takeFromRingBuffer() {
        LOCK_TAKE.lock();
        try {
            long l = consumedSeq.get() + 1;
            if (l <= knownPublishedSeq) {
                try {
                    Event<ID, DATA> eventHolder = ringBuffer.get(l);
                    try {
                        return eventHolder.get();
                    } finally {
                        eventHolder.set(null);
                    }
                } finally {
                    consumedSeq.incrementAndGet();
                }
            } else {
                knownPublishedSeq = ringBuffer.getCursor();
            }
            return null;
        } finally {
            LOCK_TAKE.unlock();
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @throws QueueException.EphemeralIsFull
     *             if the ephemeral storage is full
     */
    @Override
    public IQueueMessage<ID, DATA> take() throws QueueException.EphemeralIsFull {
        if (!isEphemeralDisabled()) {
            int ephemeralMaxSize = getEphemeralMaxSize();
            if (ephemeralMaxSize > 0 && ephemeralStorage.size() >= ephemeralMaxSize) {
                throw new QueueException.EphemeralIsFull(ephemeralMaxSize);
            }
        }
        IQueueMessage<ID, DATA> msg = takeFromRingBuffer();
        if (msg != null && !isEphemeralDisabled()) {
            ephemeralStorage.putIfAbsent(msg.getId(), msg);
        }
        return msg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<IQueueMessage<ID, DATA>> getOrphanMessages(long thresholdTimestampMs) {
        if (isEphemeralDisabled()) {
            return null;
        }
        Collection<IQueueMessage<ID, DATA>> orphanMessages = new HashSet<>();
        long now = System.currentTimeMillis();
        ephemeralStorage.forEach((key, msg) -> {
            if (msg.getQueueTimestamp().getTime() + thresholdTimestampMs < now)
                orphanMessages.add(msg);
        });
        return orphanMessages;
    }

//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public boolean moveFromEphemeralToQueueStorage(IQueueMessage<ID, DATA> _msg) {
//        if (!isEphemeralDisabled()) {
//            IQueueMessage<ID, DATA> msg = ephemeralStorage.remove(_msg.getId());
//            if (msg != null) {
//                try {
//                    putToRingBuffer(msg);
//                    return true;
//                } catch (QueueException.QueueIsFull e) {
//                    ephemeralStorage.putIfAbsent(msg.getId(), msg);
//                    return false;
//                }
//            }
//        }
//        return true;
//    }

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
        return !isEphemeralDisabled() ? ephemeralStorage.size() : 0;
    }
}

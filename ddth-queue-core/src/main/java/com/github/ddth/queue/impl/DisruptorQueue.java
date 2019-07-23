package com.github.ddth.queue.impl;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

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
 * <p>Features:</p>
 * <ul>
 * <li>Queue-size support: yes</li>
 * <li>Ephemeral storage support: yes</li>
 * <li>Ephemeral-size support: yes</li>
 * </ul>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @see <a href="https://lmax-exchange.github.io/disruptor/">LMAXDisruptor</a>
 * @since 0.4.0
 */
public class DisruptorQueue<ID, DATA> extends AbstractInmemEphemeralQueue<ID, DATA> {
    private final static class Event<ID, DATA> {
        private IQueueMessage<ID, DATA> value;

        public void set(IQueueMessage<ID, DATA> value) {
            this.value = value;
        }

        public IQueueMessage<ID, DATA> get() {
            return value;
        }
    }

    private final EventFactory<Event<ID, DATA>> EVENT_FACTORY = () -> new Event<>();
    private final Lock LOCK_TAKE = new ReentrantLock();
    private final Lock LOCK_PUT = new ReentrantLock();

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
     * Size of the ring buffer, should be power of 2.
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
     * Size of the ring buffer, should be power of 2.
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

        initEphemeralStorage(ringSize);
        consumedSeq = new Sequence();
        ringBuffer.addGatingSequences(consumedSeq);
        long cursor = ringBuffer.getCursor();
        consumedSeq.set(cursor);
        knownPublishedSeq = cursor;

        super.init();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean doPutToQueue(IQueueMessage<ID, DATA> msg, PutToQueueCase queueCase)
            throws QueueException.QueueIsFull {
        LOCK_PUT.lock();
        try {
            if (!ringBuffer.tryPublishEvent((event, _seq) -> {
                event.set(msg);
                knownPublishedSeq = _seq > knownPublishedSeq ? _seq : knownPublishedSeq;
                if (queueCase != null && queueCase != PutToQueueCase.NEW) {
                    doRemoveFromEphemeralStorage(msg);
                }
            })) {
                throw new QueueException.QueueIsFull(getRingSize());
            } else {
                return true;
            }
        } finally {
            LOCK_PUT.unlock();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage<ID, DATA> msg) {
        doRemoveFromEphemeralStorage(msg);
    }

    /**
     * Take a message from the ring buffer.
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
     * @throws QueueException.EphemeralIsFull if the ephemeral storage is full
     */
    @Override
    public IQueueMessage<ID, DATA> take() throws QueueException.EphemeralIsFull {
        ensureEphemeralSize();
        IQueueMessage<ID, DATA> msg = takeFromRingBuffer();
        doPutToEphemeralStorage(msg);
        return msg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        return (int) (ringBuffer.getCursor() - consumedSeq.get());
    }
}

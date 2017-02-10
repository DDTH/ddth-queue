package com.github.ddth.queue.qnd.disruptor;

import com.lmax.disruptor.AlertException;
import com.lmax.disruptor.InsufficientCapacityException;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;
import com.lmax.disruptor.SequenceBarrier;
import com.lmax.disruptor.TimeoutException;

public class QndDisruptor {

    private static RingBuffer<LongEvent> ringBuffer;
    private static Sequence consumedSeq;
    private static SequenceBarrier barrier;
    private static long cursor;
    private static long knownPublishedSeq;

    public static boolean offer(Long value) {
        long seq;
        try {
            seq = ringBuffer.tryNext();
        } catch (InsufficientCapacityException e1) {
            return false;
        }
        publish(value, seq);
        return true;
    }

    public static void put(Long value) {
        long seq = ringBuffer.next();
        publish(value, seq);
    }

    public static Long poll() {
        long l = consumedSeq.get() + 1;
        if (l > knownPublishedSeq) {
            updatePublishedSequence();
        }
        if (l <= knownPublishedSeq) {
            LongEvent eventHolder = ringBuffer.get(l);
            long value = eventHolder.get();
            consumedSeq.incrementAndGet();
            return value;
        }
        return null;
    }

    public static Long take() throws InterruptedException {
        long l = consumedSeq.get() + 1;
        while (knownPublishedSeq < l) {
            try {
                knownPublishedSeq = barrier.waitFor(l);
            } catch (AlertException e) {
                throw new IllegalStateException(e);
            } catch (TimeoutException e) {
                throw new IllegalStateException(e);
            }
        }
        LongEvent eventHolder = ringBuffer.get(l);
        long value = eventHolder.get();
        consumedSeq.incrementAndGet();
        return value;
    }

    private static void publish(long value, long seq) {
        LongEvent holder = ringBuffer.get(seq);
        holder.set(value);
        ringBuffer.publish(seq);
    }

    private static void updatePublishedSequence() {
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

    public static void main(String[] args) throws Exception {
        ringBuffer = RingBuffer.createSingleProducer(LongEvent.FACTORY, 4);
        consumedSeq = new Sequence();
        ringBuffer.addGatingSequences(consumedSeq);
        barrier = ringBuffer.newBarrier();
        cursor = ringBuffer.getCursor();
        consumedSeq.set(cursor);
        knownPublishedSeq = cursor;

        Long value;

        System.out.println(offer(1L));
        System.out.println(offer(2L));
        put(3L);

        value = poll();
        System.out.println(value);

        value = take();
        System.out.println(value);

    }

}

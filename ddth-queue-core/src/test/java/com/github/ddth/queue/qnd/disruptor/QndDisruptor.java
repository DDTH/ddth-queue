package com.github.ddth.queue.qnd.disruptor;

import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.Sequence;

public class QndDisruptor {

    private static RingBuffer<LongEvent> ringBuffer;
    private static Sequence consumedSeq;
    private static long knownPublishedSeq;

    static Long take() throws InterruptedException {
        long l = consumedSeq.get() + 1;
        if (l <= knownPublishedSeq) {
            LongEvent eventHolder = ringBuffer.get(l);
            long value = eventHolder.get();
            consumedSeq.incrementAndGet();
            return value;
        } else {
            knownPublishedSeq = ringBuffer.getCursor();
        }
        return null;
    }

    static boolean put(RingBuffer<LongEvent> ringBuffer, long value) {
        return ringBuffer.tryPublishEvent((event, seq) -> {
            event.set(value);
            knownPublishedSeq = seq;
        });
    }

    public static void main(String[] args) throws Exception {
        ringBuffer = RingBuffer.createSingleProducer(LongEvent.FACTORY, 4);
        consumedSeq = new Sequence();
        ringBuffer.addGatingSequences(consumedSeq);
        long cursor = ringBuffer.getCursor();
        consumedSeq.set(cursor);
        knownPublishedSeq = cursor;

        for (int i = 0; i < 100; i++) {
            boolean status = put(ringBuffer, i);
            System.out.println(i + ": " + status);
            Long value = take();
            System.out.println("Took: " + value);
        }
    }

}

package com.github.ddth.pubsub.qnd;

import java.util.Random;

import com.github.ddth.pubsub.impl.CountingSubscriber;
import com.github.ddth.queue.IMessage;

public class QndCountingSubscriber {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        Random RAND = new Random(System.currentTimeMillis());
        CountingSubscriber<Object, Object> counter = new CountingSubscriber<>();

        for (int i = 0; i < 10; i++) {
            for (int j = 0; j < 1000; j++) {
                counter.onMessage(String.valueOf(RAND.nextInt(i + 1)),
                        IMessage.EmptyMessage.INSTANCE);
            }
        }
        System.out.println("Total num msgs  : " + counter.getTotalMessages());
        System.out.println("Detailed couters: " + counter.getAllCounters());

        counter.resetCounter();

        for (int i = 0; i < 100; i++) {
            for (int j = 0; j < 10000; j++) {
                counter.onMessage(String.valueOf(RAND.nextInt(i + 1)),
                        IMessage.EmptyMessage.INSTANCE);
            }
        }
        System.out.println("Total num msgs  : " + counter.getTotalMessages());
        System.out.println("Detailed couters: " + counter.getAllCounters());
    }
}

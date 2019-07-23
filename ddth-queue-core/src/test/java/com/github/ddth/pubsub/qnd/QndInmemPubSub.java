package com.github.ddth.pubsub.qnd;

import com.github.ddth.pubsub.impl.CountingSubscriber;
import com.github.ddth.pubsub.impl.InmemPubSubHub;
import com.github.ddth.queue.IMessage;

public class QndInmemPubSub {
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {
        try (InmemPubSubHub<Object, Object> pubSubHub = new InmemPubSubHub<>()) {
            pubSubHub.init();

            CountingSubscriber<Object, Object> counter1 = new CountingSubscriber<>();
            pubSubHub.subscribe("demo", counter1);
            for (int i = 0; i < 100; i++) {
                pubSubHub.publish("demo", IMessage.EmptyMessage.INSTANCE);
            }
            System.out.println("Counter1: " + counter1.getTotalMessages());
            System.out.println("Counter1: " + counter1.getAllCounters());
            System.out.println();

            CountingSubscriber<Object, Object> counter2 = new CountingSubscriber<>();
            pubSubHub.subscribe("demo", counter1);
            pubSubHub.unsubscribe("demo1", counter1);
            pubSubHub.subscribe("demo1", counter2);
            for (int i = 0; i < 100; i++) {
                pubSubHub.publish("demo1", IMessage.EmptyMessage.INSTANCE);
            }
            System.out.println("Counter1: " + counter1.getTotalMessages());
            System.out.println("Counter1: " + counter1.getAllCounters());
            System.out.println("Counter2: " + counter2.getTotalMessages());
            System.out.println("Counter2: " + counter2.getAllCounters());
            System.out.println();

            pubSubHub.subscribe("demo", counter1);
            pubSubHub.unsubscribe("demo1", counter1);
            pubSubHub.subscribe("demo", counter2);
            for (int i = 0; i < 100; i++) {
                pubSubHub.publish("demo", IMessage.EmptyMessage.INSTANCE);
            }
            System.out.println("Counter1: " + counter1.getTotalMessages());
            System.out.println("Counter1: " + counter1.getAllCounters());
            System.out.println("Counter2: " + counter2.getTotalMessages());
            System.out.println("Counter2: " + counter2.getAllCounters());
            System.out.println();
        }
    }
}

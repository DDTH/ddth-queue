package com.github.ddth.pubsub.qnd;

import com.github.ddth.pubsub.impl.CountingSubscriber;
import com.github.ddth.pubsub.impl.universal.idint.UniversalInmemPubSubHub;

public class QndUniversalInmemPubSub {

    public static void main(String[] args) {
        try (UniversalInmemPubSubHub pubSubHub = new UniversalInmemPubSubHub()) {
            pubSubHub.init();

            CountingSubscriber<Long, byte[]> counter1 = new CountingSubscriber<>();
            pubSubHub.subscribe("demo", counter1);
            for (int i = 0; i < 100; i++) {
                pubSubHub.publish("demo", pubSubHub.createMessage());
            }
            System.out.println("Counter1: " + counter1.getTotalMessages());
            System.out.println("Counter1: " + counter1.getAllCounters());
            System.out.println();

            CountingSubscriber<Long, byte[]> counter2 = new CountingSubscriber<>();
            pubSubHub.subscribe("demo", counter1);
            pubSubHub.unsubscribe("demo1", counter1);
            pubSubHub.subscribe("demo1", counter2);
            for (int i = 0; i < 100; i++) {
                pubSubHub.publish("demo", pubSubHub.createMessage());
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
                pubSubHub.publish("demo", pubSubHub.createMessage());
            }
            System.out.println("Counter1: " + counter1.getTotalMessages());
            System.out.println("Counter1: " + counter1.getAllCounters());
            System.out.println("Counter2: " + counter2.getTotalMessages());
            System.out.println("Counter2: " + counter2.getAllCounters());
            System.out.println();
        }
    }

}

package com.github.ddth.pubsub.qnd;

import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.pubsub.impl.CountingSubscriber;
import com.github.ddth.pubsub.impl.universal.idint.UniversalRedisPubSubHub;

public class QndUniversalRedisPubSub {

    public static void main(String[] args) throws Exception {
        try (JedisConnector jc = new JedisConnector()) {
            jc.setRedisHostsAndPorts("localhost:6379");
            jc.init();

            try (UniversalRedisPubSubHub pubSubHub = new UniversalRedisPubSubHub()) {
                pubSubHub.setJedisConnector(jc).init();
                while (!pubSubHub.isReady()) {
                    Thread.sleep(1);
                }

                CountingSubscriber<Long, byte[]> counter1 = new CountingSubscriber<>();
                pubSubHub.subscribe("demo", counter1);
                for (int i = 0; i < 100; i++) {
                    pubSubHub.publish("demo", pubSubHub.createMessage());
                }
                Thread.sleep(1000);
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
                Thread.sleep(1000);
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
                Thread.sleep(1000);
                System.out.println("Counter1: " + counter1.getTotalMessages());
                System.out.println("Counter1: " + counter1.getAllCounters());
                System.out.println("Counter2: " + counter2.getTotalMessages());
                System.out.println("Counter2: " + counter2.getAllCounters());
                System.out.println();
            }
        }
    }

}

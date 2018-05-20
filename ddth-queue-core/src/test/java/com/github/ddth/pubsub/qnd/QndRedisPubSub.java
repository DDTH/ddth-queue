package com.github.ddth.pubsub.qnd;

import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.pubsub.impl.CountingSubscriber;
import com.github.ddth.pubsub.impl.RedisPubSubHub;
import com.github.ddth.queue.IMessage;

public class QndRedisPubSub {

    @SuppressWarnings("unchecked")
    public static void main(String[] args) throws Exception {
        try (JedisConnector jc = new JedisConnector()) {
            jc.setRedisHostsAndPorts("localhost:6379");
            jc.init();

            try (RedisPubSubHub<Object, Object> pubSubHub = new RedisPubSubHub<>()) {
                pubSubHub.setJedisConnector(jc).init();
                while (!pubSubHub.isReady()) {
                    Thread.sleep(1);
                }

                CountingSubscriber<Object, Object> counter1 = new CountingSubscriber<>();
                pubSubHub.subscribe("demo", counter1);
                for (int i = 0; i < 100; i++) {
                    pubSubHub.publish("demo", IMessage.EmptyMessage.INSTANCE);
                }
                Thread.sleep(1000);
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
                    pubSubHub.publish("demo", IMessage.EmptyMessage.INSTANCE);
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

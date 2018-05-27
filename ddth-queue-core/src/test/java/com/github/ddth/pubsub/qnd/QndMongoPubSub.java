package com.github.ddth.pubsub.qnd;

import com.github.ddth.pubsub.impl.CountingSubscriber;
import com.github.ddth.pubsub.impl.MongodbPubSubHub;
import com.github.ddth.pubsub.impl.universal.UniversalIdIntMessage;
import com.github.ddth.pubsub.impl.universal.UniversalIdIntMessageFactory;
import com.github.ddth.queue.IMessage;
import com.github.ddth.queue.utils.MongoUtils;

public class QndMongoPubSub {
    private static class MyMongodbPubSubHub extends MongodbPubSubHub<Long, byte[]> {
        protected IMessage<Long, byte[]> deserialize(byte[] msgData) {
            return deserialize(msgData, UniversalIdIntMessage.class);
        }

        public MyMongodbPubSubHub init() {
            super.init();

            setMessageFactory(UniversalIdIntMessageFactory.INSTANCE);

            MongoUtils.dropCollection(getDatabase(), "demo");
            MongoUtils.dropCollection(getDatabase(), "demo1");
            MongoUtils.dropCollection(getDatabase(), "demo2");
            return this;
        }
    }

    public static void main(String[] args) throws Exception {
        try (MongodbPubSubHub<Long, byte[]> pubSubHub = new MyMongodbPubSubHub()) {
            pubSubHub.setConnectionString("mongodb://test:test@localhost:27017/test")
                    .setDatabaseName("test");
            pubSubHub.init();

            CountingSubscriber<Long, byte[]> counter1 = new CountingSubscriber<>();
            pubSubHub.subscribe("demo1", counter1);
            Thread.sleep(100);
            for (int i = 0; i < 10; i++) {
                IMessage<Long, byte[]> msg = pubSubHub.createMessage((long) i + 1, "".getBytes());
                pubSubHub.publish("demo1", msg);
            }
            Thread.sleep(1000);
            System.out.println("Counter1: " + counter1.getTotalMessages());
            System.out.println("Counter1: " + counter1.getAllCounters());
            System.out.println();

            CountingSubscriber<Long, byte[]> counter2 = new CountingSubscriber<>();
            pubSubHub.subscribe("demo1", counter1);
            pubSubHub.unsubscribe("demo2", counter1);
            pubSubHub.subscribe("demo2", counter2);
            Thread.sleep(100);
            for (int i = 0; i < 10; i++) {
                IMessage<Long, byte[]> msg = pubSubHub.createMessage((long) i + 11, "".getBytes());
                pubSubHub.publish("demo2", msg);
            }
            Thread.sleep(1000);
            System.out.println("Counter1: " + counter1.getTotalMessages());
            System.out.println("Counter1: " + counter1.getAllCounters());
            System.out.println("Counter2: " + counter2.getTotalMessages());
            System.out.println("Counter2: " + counter2.getAllCounters());
            System.out.println();

            pubSubHub.subscribe("demo1", counter1);
            pubSubHub.unsubscribe("demo2", counter1);
            pubSubHub.subscribe("demo2", counter2);
            Thread.sleep(100);
            for (int i = 0; i < 10; i++) {
                IMessage<Long, byte[]> msg = pubSubHub.createMessage((long) i + 21, "".getBytes());
                pubSubHub.publish("demo1", msg);
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

package com.github.ddth.qnd.mongodb;

import java.util.Date;
import java.util.function.Consumer;

import org.bson.Document;
import org.bson.types.Binary;

import com.github.ddth.commons.utils.DateFormatUtils;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.internal.utils.QueueUtils;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.FindOneAndUpdateOptions;
import com.mongodb.client.model.Sorts;
import com.mongodb.client.model.Updates;

public class QndMongoDb {

    public final static String COLLECTION_FIELD_ID = "id";
    public final static String COLLECTION_FIELD_TIME = "time";
    public final static String COLLECTION_FIELD_QUEUE_TIME = "queue_time";
    public final static String COLLECTION_FIELD_QUEUE_DATA = "data";
    public final static String COLLECTION_FIELD_EPHEMERAL_KEY = "ekey";

    static byte[] serialize(IQueueMessage<?, ?> msg) {
        return SerializationUtils.toByteArray(msg);
    }

    static Document toDocument(IQueueMessage<?, ?> msg) {
        return new Document(COLLECTION_FIELD_ID, msg.getId())
                .append(COLLECTION_FIELD_EPHEMERAL_KEY, null)
                .append(COLLECTION_FIELD_TIME, msg.getTimestamp())
                .append(COLLECTION_FIELD_QUEUE_TIME, msg.getQueueTimestamp())
                .append(COLLECTION_FIELD_QUEUE_DATA, serialize(msg));
    }

    private static void queue(MongoCollection<Document> collection, IQueueMessage<?, ?> msg) {
        collection.insertOne(toDocument(msg));
    }

    private static IQueueMessage<Long, byte[]> take(MongoCollection<Document> collection) {
        String ephemeralId = QueueUtils.IDGEN.generateId128Hex().toLowerCase();
        Document doc = collection.findOneAndUpdate(Filters.eq(COLLECTION_FIELD_EPHEMERAL_KEY, null),
                Updates.set(COLLECTION_FIELD_EPHEMERAL_KEY, ephemeralId),
                new FindOneAndUpdateOptions().sort(Sorts.ascending(COLLECTION_FIELD_QUEUE_TIME)));
        if (doc != null) {
            byte[] data = doc.get(COLLECTION_FIELD_QUEUE_DATA, Binary.class).getData();
            return SerializationUtils.fromByteArray(data, UniversalIdIntQueueMessage.class);
        }
        return null;
    }

    public static void main(String[] args) {
        MongoClient client = MongoClients.create("mongodb://test:test@localhost:27017/test");
        try {
            MongoCollection<Document> collection = client.getDatabase("test")
                    .getCollection("ddth_queue");
            System.out.println("Storage Size: "
                    + collection.count(Filters.eq(COLLECTION_FIELD_EPHEMERAL_KEY, null)));
            System.out.println("Ephemeral Size: "
                    + collection.count(Filters.ne(COLLECTION_FIELD_EPHEMERAL_KEY, null)));

            Consumer<Document> consumer = (doc) -> System.out.println(doc);
            Date filter = new Date(System.currentTimeMillis() - 16 * 3600 * 1000L);
            System.out.println(DateFormatUtils.toString(filter, DateFormatUtils.DF_ISO8601));
            collection.find(Filters.lte(COLLECTION_FIELD_QUEUE_TIME, filter)).forEach(consumer);

            // System.out.println("Before: " + collection.count());
            //
            // // UniversalIdIntQueueMessage msg =
            // // UniversalIdIntQueueMessage.newInstance("demo");
            // // queue(collection, msg);
            //
            // System.out.println("After: " + collection.count());
            //
            // IQueueMessage<?, ?> msg2 = take(collection);
            // System.out.println(msg2);
        } finally {
            client.close();
        }
    }

}

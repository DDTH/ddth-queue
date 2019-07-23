package com.github.ddth.qnd.mongodb;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;

import org.bson.Document;

import com.github.ddth.qnd.utils.MongoUtils;
import com.mongodb.CursorType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Sorts;

public class QndMongoTailableCursor {

    public static void main(String[] args) throws Exception {
        final long MAX_DOCS = 10;
        final long MAX_SIZE = 64 * 1024;
        final Random RAND = new Random(System.currentTimeMillis());
        final AtomicBoolean CLOSED = new AtomicBoolean(false);

        MongoClient client = MongoClients.create("mongodb://test:test@localhost:27017/test");
        try {
            MongoDatabase database = client.getDatabase("test");
            MongoUtils.dropCollection(database, "ddth_pubsub");
            MongoCollection<Document> collection = MongoUtils.createCollection(database,
                    "ddth_pubsub", new CreateCollectionOptions().capped(true).maxDocuments(MAX_DOCS)
                            .sizeInBytes(MAX_SIZE));

            collection.insertOne(new Document().append("id", 0).append("time", new Date()));
            collection.insertOne(new Document().append("id", 0).append("time", new Date()));
            collection.insertOne(new Document().append("id", 0).append("time", new Date()));
            System.out.println("Storage Size: " + collection.count());

            MongoCursor<Document> cursor = collection.find().cursorType(CursorType.TailableAwait)
                    .sort(Sorts.ascending("$natural")).skip((int) 1).iterator();
            new Thread() {
                public void run() {
                    while (!CLOSED.get()) {
                        System.out.println("Ping");
                        // synchronized (cursor) {
                        if (!CLOSED.get()) {
                            try {
                                System.out.println(cursor.tryNext());
                            } catch (IllegalStateException e) {
                            }
                        }
                        // }
                    }
                }
            }.start();

            for (int i = 1; i < 10; i++) {
                collection.insertOne(new Document().append("id", i).append("time", new Date()));
                Thread.sleep(RAND.nextInt(1000));
            }

            Thread.sleep(5000);

            CLOSED.set(true);
            cursor.close();
            System.out.println("Storage Size: " + collection.count());
        } finally {
            client.close();
        }
    }

}

package com.github.ddth.queue.internal.utils;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Filters;
import org.bson.Document;

/**
 * MongoDB utility class.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.1
 */
public class MongoUtils {
    /**
     * Check if a collection exists.
     *
     * @param db
     * @param collectionName
     * @return
     */
    public static boolean collectionExists(MongoDatabase db, String collectionName) {
        return db.listCollections().filter(Filters.eq("name", collectionName)).first() != null;
    }

    /**
     * Create a new collection.
     *
     * @param db
     * @param collectionName
     * @param options
     * @return
     */
    public static MongoCollection<Document> createCollection(MongoDatabase db, String collectionName,
            CreateCollectionOptions options) {
        if (options != null) {
            db.createCollection(collectionName, options);
        } else {
            db.createCollection(collectionName);
        }
        return db.getCollection(collectionName);
    }
}

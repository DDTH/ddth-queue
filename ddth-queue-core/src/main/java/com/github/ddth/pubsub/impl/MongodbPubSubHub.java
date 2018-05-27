package com.github.ddth.pubsub.impl;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.types.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.ISubscriber;
import com.github.ddth.queue.IMessage;
import com.github.ddth.queue.utils.MongoUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import com.google.common.cache.RemovalNotification;
import com.mongodb.CursorType;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.CreateCollectionOptions;
import com.mongodb.client.model.Sorts;

/**
 * (Experimental) MongoDB implementation of {@link IPubSubHub}.
 * 
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.1
 */
public class MongodbPubSubHub<ID, DATA> extends AbstractPubSubHub<ID, DATA> {

    private final Logger LOGGER = LoggerFactory.getLogger(MongodbPubSubHub.class);

    public final static String DEFAULT_CONN_STR = "mongodb://localhost:27017/local";
    public final static String DEFAULT_DATABASE_NAME = "ddth_pubsub";
    public final static long DEFAULT_MAX_DOCUMENTS = Runtime.getRuntime().availableProcessors() * 2;
    public final static long DEFAULT_MAX_COLLECTION_SIZE = DEFAULT_MAX_DOCUMENTS * 1024;

    private MongoClient mongoClient;
    private boolean myOwnMongoClient = true;
    private String connectionString = DEFAULT_CONN_STR;
    private String databaseName = DEFAULT_DATABASE_NAME;
    private long maxDocuments = DEFAULT_MAX_DOCUMENTS;
    private long maxCollectionSize = DEFAULT_MAX_COLLECTION_SIZE;

    /**
     * Max number of document per pub/sub collection.
     * 
     * <p>
     * To allow pub/sub, the collection must be capped with a max number of
     * documents.
     * </p>
     * 
     * @return
     * @see #DEFAULT_MAX_DOCUMENTS
     */
    public long getMaxDocuments() {
        return maxDocuments;
    }

    /**
     * Max number of document per pub/sub collection.
     * 
     * <p>
     * To allow pub/sub, the collection must be capped with a max number of
     * documents.
     * </p>
     * 
     * @param maxDocuments
     * @return
     * @see #DEFAULT_MAX_DOCUMENTS
     */
    public MongodbPubSubHub<ID, DATA> setMaxDocuments(long maxDocuments) {
        this.maxDocuments = maxDocuments;
        return this;
    }

    /**
     * Max collection's size in bytes.
     * 
     * <p>
     * To allow pub/sub, the collection must be capped with a max size.
     * </p>
     * 
     * @return
     * @see #DEFAULT_MAX_COLLECTION_SIZE
     */
    public long getMaxCollectionSize() {
        return maxCollectionSize;
    }

    /**
     * Max number of document per pub/sub collection.
     * 
     * <p>
     * To allow pub/sub, the collection must be capped with a max size.
     * </p>
     * 
     * @param maxCollectionSize
     * @return
     * @see #DEFAULT_MAX_COLLECTION_SIZE
     */
    public MongodbPubSubHub<ID, DATA> setMaxCollectionSize(long maxCollectionSize) {
        this.maxCollectionSize = maxCollectionSize;
        return this;
    }

    /**
     * Getter for {@link #connectionString} (see
     * http://mongodb.github.io/mongo-java-driver/3.7/driver/getting-started/quick-start/).
     *
     * @return
     */
    public String getConnectionString() {
        return connectionString;
    }

    /**
     * Setter for {@link #connectionString} (see
     * http://mongodb.github.io/mongo-java-driver/3.7/driver/getting-started/quick-start/).
     *
     * @param connectionString
     * @return
     */
    public MongodbPubSubHub<ID, DATA> setConnectionString(String connectionString) {
        this.connectionString = connectionString;
        return this;
    }

    /**
     * Getter for {@link #databaseName}.
     *
     * @return
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Setter for {@link #databaseName}.
     *
     * @param databaseName
     * @return
     */
    public MongodbPubSubHub<ID, DATA> setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    /**
     * Getter for {@link #mongoClient}.
     *
     * @return
     */
    protected MongoClient getMongoClient() {
        return mongoClient;
    }

    /**
     * Setter for {@link #mongoClient}.
     *
     * @param mongoClient
     * @return
     */
    public MongodbPubSubHub<ID, DATA> setMongoClient(MongoClient mongoClient) {
        return setMongoClient(mongoClient, false);
    }

    /**
     * Setter for {@link #mongoClient}.
     *
     * @param mongoClient
     * @param setMyOwnMongoClient
     * @return
     */
    protected MongodbPubSubHub<ID, DATA> setMongoClient(MongoClient mongoClient,
            boolean setMyOwnMongoClient) {
        if (myOwnMongoClient && this.mongoClient != null) {
            this.mongoClient.close();
        }
        this.mongoClient = mongoClient;
        myOwnMongoClient = setMyOwnMongoClient;
        return this;
    }

    private MongoDatabase database;

    protected MongoDatabase getDatabase() {
        if (database == null) {
            synchronized (this) {
                if (database == null) {
                    database = mongoClient.getDatabase(getDatabaseName());
                }
            }
        }
        return database;
    }

    /*----------------------------------------------------------------------*/
    private LoadingCache<String, MongoPubSubGateway> cursors = CacheBuilder.newBuilder()
            .removalListener(new RemovalListener<String, MongoPubSubGateway>() {
                @Override
                public void onRemoval(
                        RemovalNotification<String, MongoPubSubGateway> notification) {
                    notification.getValue().destroy();
                }
            }).build(new CacheLoader<String, MongoPubSubGateway>() {
                @Override
                public MongoPubSubGateway load(String key) throws Exception {
                    MongoPubSubGateway gateway = new MongoPubSubGateway(key);
                    gateway.run();
                    return gateway;
                }
            });

    public final static String COLLECTION_FIELD_ID = "id";
    public final static String COLLECTION_FIELD_TIME = "time";
    public final static String COLLECTION_FIELD_DATA = "data";

    protected Document toDocument(IMessage<ID, DATA> msg) {
        return new Document(COLLECTION_FIELD_ID, msg.getId())
                .append(COLLECTION_FIELD_TIME, msg.getTimestamp())
                .append(COLLECTION_FIELD_DATA, serialize(msg));
    }

    protected IMessage<ID, DATA> fromDocument(Document doc) {
        return doc != null ? deserialize(doc.get(COLLECTION_FIELD_DATA, Binary.class).getData())
                : null;
    }

    private class MongoPubSubGateway {
        private MongoCollection<Document> collection;
        private MongoCursor<Document> cursor;
        private String channel;
        private boolean stopped = false;
        private Set<ISubscriber<ID, DATA>> subscriptions = new HashSet<>();

        public MongoPubSubGateway(String channel) {
            this.channel = channel;
        }

        public boolean publish(IMessage<ID, DATA> msg) {
            collection.insertOne(toDocument(msg));
            return true;
        }

        public void subscribe(ISubscriber<ID, DATA> subscriber) {
            synchronized (subscriptions) {
                subscriptions.add(subscriber);
            }
        }

        public void unsubscribe(ISubscriber<ID, DATA> subscriber) {
            synchronized (subscriptions) {
                subscriptions.remove(subscriber);
            }
        }

        public void run() {
            boolean collectionExists = MongoUtils.collectionExists(getDatabase(), channel);
            if (!collectionExists) {
                this.collection = MongoUtils.createCollection(getDatabase(), channel,
                        new CreateCollectionOptions().capped(true).maxDocuments(getMaxDocuments())
                                .sizeInBytes(getMaxCollectionSize()));
                /*
                 * Once the collection is newly created, insert a dummy message
                 */
                publish(createMessage(null, null));
            } else {
                this.collection = getDatabase().getCollection(channel);
            }
            this.cursor = collection.find().cursorType(CursorType.TailableAwait)
                    .sort(Sorts.ascending("$natural")).iterator();

            new Thread() {
                public void run() {
                    while (!stopped) {
                        try {
                            Document doc = null;
                            try {
                                doc = cursor.next();
                            } catch (IllegalStateException e) {
                                LOGGER.warn(e.getMessage(), e);
                            }
                            IMessage<ID, DATA> message = fromDocument(doc);
                            if (message == null
                                    || (message.getId() == null && message.getData() == null)) {
                                // ignore dummy message
                                continue;
                            }
                            Collection<ISubscriber<ID, DATA>> subs;
                            synchronized (subscriptions) {
                                subs = new LinkedList<>(subscriptions);
                            }
                            try {
                                subs.forEach((sub) -> sub.onMessage(channel, message));
                            } catch (Exception e) {
                                LOGGER.warn(e.getMessage(), e);
                            }
                        } catch (Exception e) {
                            LOGGER.error(e.getMessage(), e);
                        }
                    }
                }
            }.start();
        }

        public void destroy() {
            try {
                stopped = true;
                cursor.close();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
    }

    /**
     * Build a new {@link MongoClient} instance.
     *
     * @return
     */
    protected MongoClient buildMongoClient() {
        String connectionString = getConnectionString();
        if (StringUtils.isBlank(connectionString)) {
            throw new IllegalStateException("MongoDB ConnectionString is not defined.");
        }
        MongoClient mc = MongoClients.create(connectionString);
        return mc;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public MongodbPubSubHub<ID, DATA> init() {
        if (getMongoClient() == null) {
            setMongoClient(buildMongoClient(), true);
        }

        super.init();

        if (getMongoClient() == null) {
            throw new IllegalStateException("MongoDB Client is null.");
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        try {
            super.destroy();
        } finally {
            try {
                cursors.invalidateAll();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            }

            if (mongoClient != null && myOwnMongoClient) {
                try {
                    mongoClient.close();
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                } finally {
                    mongoClient = null;
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean publish(String channel, IMessage<ID, DATA> msg) {
        try {
            return cursors.get(channel).publish(msg);
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage(), e);
            return false;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(String channel, ISubscriber<ID, DATA> subscriber) {
        try {
            cursors.get(channel).subscribe(subscriber);
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unsubscribe(String channel, ISubscriber<ID, DATA> subscriber) {
        try {
            cursors.get(channel).unsubscribe(subscriber);
        } catch (ExecutionException e) {
            LOGGER.error(e.getMessage(), e);
        }
    }

}

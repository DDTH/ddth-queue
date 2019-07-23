package com.github.ddth.queue.impl;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.internal.utils.MongoUtils;
import com.github.ddth.queue.internal.utils.QueueUtils;
import com.github.ddth.queue.utils.QueueException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.apache.commons.lang3.StringUtils;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.Binary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.function.Consumer;

/**
 * (Experimental) MongoDB implementation of {@link IQueue}.
 *
 * <ul>
 * <li>Queue-size support: yes</li>
 * <li>Ephemeral storage support: yes</li>
 * <li>Ephemeral-size support: yes</li>
 * </ul>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.1
 */
public abstract class MongodbQueue<ID, DATA> extends AbstractEphemeralSupportQueue<ID, DATA> {

    private final Logger LOGGER = LoggerFactory.getLogger(MongodbQueue.class);

    public final static String DEFAULT_CONN_STR = "mongodb://localhost:27017/local";
    public final static String DEFAULT_COLLECTION_NAME = "ddth_queue";

    private MongoClient mongoClient;
    private boolean myOwnMongoClient = true;
    private String connectionString = DEFAULT_CONN_STR;
    private String databaseName;
    private String collectionName = DEFAULT_COLLECTION_NAME;

    /**
     * MongoDB's connection string (see http://mongodb.github.io/mongo-java-driver/3.10/driver/getting-started/quick-start/).
     *
     * @return
     */
    public String getConnectionString() {
        return connectionString;
    }

    /**
     * MongoDB's connection string (see http://mongodb.github.io/mongo-java-driver/3.10/driver/getting-started/quick-start/).
     *
     * @param connectionString
     * @return
     */
    public MongodbQueue<ID, DATA> setConnectionString(String connectionString) {
        this.connectionString = connectionString;
        return this;
    }

    /**
     * Name of MongoDB database to store data.
     *
     * @return
     */
    public String getDatabaseName() {
        return databaseName;
    }

    /**
     * Name of MongoDB database to store data.
     *
     * @param databaseName
     * @return
     */
    public MongodbQueue<ID, DATA> setDatabaseName(String databaseName) {
        this.databaseName = databaseName;
        return this;
    }

    /**
     * Name of MongoDB collection to store queue messages.
     *
     * @return
     */
    public String getCollectionName() {
        return collectionName;
    }

    /**
     * Name of MongoDB collection to store queue messages.
     *
     * @param collectionName
     * @return
     */
    public MongodbQueue<ID, DATA> setCollectionName(String collectionName) {
        this.collectionName = collectionName;
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
    public MongodbQueue<ID, DATA> setMongoClient(MongoClient mongoClient) {
        return setMongoClient(mongoClient, false);
    }

    /**
     * Setter for {@link #mongoClient}.
     *
     * @param mongoClient
     * @param setMyOwnMongoClient
     * @return
     */
    protected MongodbQueue<ID, DATA> setMongoClient(MongoClient mongoClient, boolean setMyOwnMongoClient) {
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

    private MongoCollection<Document> collection;

    protected MongoCollection<Document> getCollection() {
        if (collection == null) {
            synchronized (this) {
                if (collection == null) {
                    collection = getDatabase().getCollection(getCollectionName());
                }
            }
        }
        return collection;
    }

    /*----------------------------------------------------------------------*/

    /**
     * Initialize collection:
     *
     * <ul>
     * <li>Check if collection exists.</li>
     * <li>If collection does not exist, create collection and indexes.</li>
     * </ul>
     */
    protected void initCollection() {
        boolean collectionExists = MongoUtils.collectionExists(getDatabase(), getCollectionName());
        if (!collectionExists) {
            LOGGER.info("Creating collection [" + getCollectionName() + "]...");
            MongoUtils.createCollection(getDatabase(), getCollectionName(), null);
            MongoCollection<?> collection = getCollection();

            LOGGER.info("Creating index for field [" + getCollectionName() + "." + MongodbQueue.COLLECTION_FIELD_ID
                    + "]...");
            collection.createIndex(new Document().append(MongodbQueue.COLLECTION_FIELD_ID, 1),
                    new IndexOptions().unique(true));

            LOGGER.info("Creating index for field [" + getCollectionName() + "."
                    + MongodbQueue.COLLECTION_FIELD_EPHEMERAL_KEY + "]...");
            collection.createIndex(new Document().append(MongodbQueue.COLLECTION_FIELD_EPHEMERAL_KEY, 1),
                    new IndexOptions());

            LOGGER.info(
                    "Creating index for field [" + getCollectionName() + "." + MongodbQueue.COLLECTION_FIELD_QUEUE_TIME
                            + "]...");
            collection.createIndex(new Document().append(MongodbQueue.COLLECTION_FIELD_QUEUE_TIME, 1),
                    new IndexOptions());

            LOGGER.info("Creating index for field [" + getCollectionName() + "." + MongodbQueue.COLLECTION_FIELD_TIME
                    + "]...");
            collection.createIndex(new Document().append(MongodbQueue.COLLECTION_FIELD_TIME, 1), new IndexOptions());

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
        return MongoClients.create(connectionString);
    }

    /**
     * Init method.
     *
     * @return
     * @throws Exception
     */
    public MongodbQueue<ID, DATA> init() throws Exception {
        if (getMongoClient() == null) {
            setMongoClient(buildMongoClient(), true);
        }

        super.init();

        if (getMongoClient() == null) {
            throw new IllegalStateException("MongoDB Client is null.");
        }

        initCollection();

        return this;
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        try {
            super.destroy();
        } finally {
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

    /*--------------------------------------------------*/

    public final static String COLLECTION_FIELD_ID = "id";
    public final static String COLLECTION_FIELD_TIME = "time";
    public final static String COLLECTION_FIELD_QUEUE_TIME = "queue_time";
    public final static String COLLECTION_FIELD_QUEUE_DATA = "data";
    public final static String COLLECTION_FIELD_EPHEMERAL_KEY = "ekey";

    protected Document toDocument(IQueueMessage<ID, DATA> msg) {
        return new Document(COLLECTION_FIELD_ID, msg.getId()).append(COLLECTION_FIELD_EPHEMERAL_KEY, null)
                .append(COLLECTION_FIELD_TIME, msg.getTimestamp())
                .append(COLLECTION_FIELD_QUEUE_TIME, msg.getQueueTimestamp())
                .append(COLLECTION_FIELD_QUEUE_DATA, serialize(msg));
    }

    protected IQueueMessage<ID, DATA> fromDocument(Document doc) {
        return doc != null ? deserialize(doc.get(COLLECTION_FIELD_QUEUE_DATA, Binary.class).getData()) : null;
    }

    private final static ReplaceOptions REPLACE_OPTIONS = new ReplaceOptions().upsert(true);

    /**
     * Insert/Update the message to collection.
     *
     * @param msg
     * @return
     */
    protected boolean upsertToCollection(IQueueMessage<ID, DATA> msg) {
        getCollection().replaceOne(Filters.eq(COLLECTION_FIELD_ID, msg.getId()), toDocument(msg), REPLACE_OPTIONS);
        return true;
    }

    /**
     * Insert a new message to collection.
     *
     * @param msg
     * @return
     */
    protected boolean insertToCollection(IQueueMessage<ID, DATA> msg) {
        getCollection().insertOne(toDocument(msg));
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean doPutToQueue(IQueueMessage<ID, DATA> msg, PutToQueueCase queueCase) {
        return queueCase == null || queueCase == PutToQueueCase.NEW || isEphemeralDisabled() ?
                insertToCollection(msg) :
                upsertToCollection(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage<ID, DATA> msg) {
        if (!isEphemeralDisabled()) {
            getCollection().deleteOne(Filters.and(Filters.ne(COLLECTION_FIELD_EPHEMERAL_KEY, null),
                    Filters.eq(COLLECTION_FIELD_ID, msg.getId())));
        }
    }

    private final static Bson FILTER_TAKE = Filters.eq(COLLECTION_FIELD_EPHEMERAL_KEY, null);
    private final static FindOneAndUpdateOptions TAKE_OPTIONS = new FindOneAndUpdateOptions()
            .sort(Sorts.ascending(COLLECTION_FIELD_QUEUE_TIME));
    private final static FindOneAndDeleteOptions TAKE_EPHEMERAL_DISABLED_OPTIONS = new FindOneAndDeleteOptions()
            .sort(Sorts.ascending(COLLECTION_FIELD_QUEUE_TIME));

    /**
     * {@inheritDoc}
     *
     * @throws QueueException.EphemeralIsFull if the ephemeral storage is full
     */
    @Override
    public IQueueMessage<ID, DATA> take() throws QueueException.EphemeralIsFull {
        Document document;
        if (isEphemeralDisabled()) {
            document = getCollection().findOneAndDelete(FILTER_TAKE, TAKE_EPHEMERAL_DISABLED_OPTIONS);
        } else {
            int ephemeralMaxSize = getEphemeralMaxSize();
            if (ephemeralMaxSize > 0 && ephemeralSize() >= ephemeralMaxSize) {
                throw new QueueException.EphemeralIsFull(ephemeralMaxSize);
            }
            String ephemeralId = QueueUtils.IDGEN.generateId128Hex();
            document = getCollection()
                    .findOneAndUpdate(FILTER_TAKE, Updates.set(COLLECTION_FIELD_EPHEMERAL_KEY, ephemeralId),
                            TAKE_OPTIONS);
        }
        return fromDocument(document);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<IQueueMessage<ID, DATA>> getOrphanMessages(long thresholdTimestampMs) {
        Collection<IQueueMessage<ID, DATA>> orphanMessages = new HashSet<>();
        if (!isEphemeralDisabled()) {
            Consumer<Document> processor = (doc) -> orphanMessages.add(fromDocument(doc));
            Date threshold = new Date(System.currentTimeMillis() - thresholdTimestampMs);
            getCollection().find(Filters.and(Filters.ne(COLLECTION_FIELD_EPHEMERAL_KEY, null),
                    Filters.lte(COLLECTION_FIELD_QUEUE_TIME, threshold))).forEach(processor);
        }
        return orphanMessages;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        return (int) getCollection().countDocuments(Filters.eq(COLLECTION_FIELD_EPHEMERAL_KEY, null));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        return isEphemeralDisabled() ?
                0 :
                (int) getCollection().countDocuments(Filters.ne(COLLECTION_FIELD_EPHEMERAL_KEY, null));
    }
}

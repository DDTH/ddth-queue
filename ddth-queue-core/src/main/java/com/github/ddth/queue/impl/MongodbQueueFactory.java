package com.github.ddth.queue.impl;

import com.github.ddth.queue.QueueSpec;
import com.mongodb.client.MongoClient;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Factory to create {@link MongodbQueue} instances.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.1
 */
public abstract class MongodbQueueFactory<T extends MongodbQueue<ID, DATA>, ID, DATA>
        extends AbstractQueueFactory<T, ID, DATA> {

    private final Logger LOGGER = LoggerFactory.getLogger(MongodbQueueFactory.class);

    public final static String SPEC_FIELD_CONNECTION_STRING = "conn_string";
    public final static String SPEC_FIELD_DATABASE_NAME = "database";
    public final static String SPEC_FIELD_COLLECTION_NAME = "collection";

    private MongoClient defaultMongoClient;
    private boolean myOwnMongoClient;
    private String defaultConnectionString = MongodbQueue.DEFAULT_CONN_STR, defaultCollectionName = MongodbQueue.DEFAULT_COLLECTION_NAME, defaultDatabaseName;

    /**
     * Default MongoDB's connection string (see http://mongodb.github.io/mongo-java-driver/3.10/driver/getting-started/quick-start/), passed to all queues created by this factory.
     *
     * @return
     */
    public String getDefaultConnectionString() {
        return defaultConnectionString;
    }

    /**
     * Default MongoDB's connection string (see http://mongodb.github.io/mongo-java-driver/3.10/driver/getting-started/quick-start/), passed to all queues created by this factory.
     *
     * @param defaultConnectionString
     * @return
     */
    public MongodbQueueFactory<T, ID, DATA> setDefaultConnectionString(String defaultConnectionString) {
        this.defaultConnectionString = defaultConnectionString;
        return this;
    }

    /**
     * Default name of MongoDB database to store data, passed to all queues created by this factory.
     *
     * @return
     */
    public String getDefaultDatabaseName() {
        return defaultDatabaseName;
    }

    /**
     * Default name of MongoDB database to store data, passed to all queues created by this factory.
     *
     * @param defaultDatabaseName
     * @return
     */
    public MongodbQueueFactory<T, ID, DATA> setDefaultDatabaseName(String defaultDatabaseName) {
        this.defaultDatabaseName = defaultDatabaseName;
        return this;
    }

    /**
     * Default name of MongoDB collection to store queue messages, passed to all queues created by this factory.
     *
     * @return
     */
    public String getDefaultCollectionName() {
        return defaultCollectionName;
    }

    /**
     * Default name of MongoDB collection to store queue messages, passed to all queues created by this factory.
     *
     * @param defaultCollectionName
     * @return
     */
    public MongodbQueueFactory<T, ID, DATA> setDefaultCollectionName(String defaultCollectionName) {
        this.defaultCollectionName = defaultCollectionName;
        return this;
    }

    /**
     * If all {@link MongodbQueue} instances are connecting to one MongoDB
     * server or cluster, it's a good idea to pre-create a {@link MongoClient}
     * instance and share it amongst {@link MongodbQueue} instances created from
     * this factory by assigning it to {@link #defaultMongoClient} (see
     * {@link #setDefaultMongoClient(MongoClient)}).
     *
     * @return
     */
    protected MongoClient getDefaultMongoClient() {
        return defaultMongoClient;
    }

    /**
     * If all {@link MongodbQueue} instances are connecting to one MongoDB
     * server or cluster, it's a good idea to pre-create a {@link MongoClient}
     * instance and share it amongst {@link MongodbQueue} instances created from
     * this factory by assigning it to {@link #defaultMongoClient} (see
     * {@link #setDefaultMongoClient(MongoClient)}).
     *
     * @param mongoClient
     * @return
     */
    public MongodbQueueFactory<T, ID, DATA> setDefaultMongoClient(MongoClient mongoClient) {
        return setDefaultMongoClient(mongoClient, false);
    }

    /**
     * If all {@link MongodbQueue} instances are connecting to one MongoDB
     * server or cluster, it's a good idea to pre-create a {@link MongoClient}
     * instance and share it amongst {@link MongodbQueue} instances created from
     * this factory by assigning it to {@link #defaultMongoClient} (see
     * {@link #setDefaultMongoClient(MongoClient)}).
     *
     * @param mongoClient
     * @param setMyOwnMongoClient
     * @return
     */
    protected MongodbQueueFactory<T, ID, DATA> setDefaultMongoClient(MongoClient mongoClient,
            boolean setMyOwnMongoClient) {
        if (myOwnMongoClient && this.defaultMongoClient != null) {
            this.defaultMongoClient.close();
        }
        this.defaultMongoClient = mongoClient;
        myOwnMongoClient = setMyOwnMongoClient;
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
            if (defaultMongoClient != null && myOwnMongoClient) {
                try {
                    defaultMongoClient.close();
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                } finally {
                    defaultMongoClient = null;
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws Exception
     */
    @Override
    protected void initQueue(T queue, QueueSpec spec) throws Exception {
        queue.setMongoClient(defaultMongoClient).setConnectionString(defaultConnectionString)
                .setCollectionName(defaultCollectionName);

        String connectionString = spec.getField(SPEC_FIELD_CONNECTION_STRING);
        if (!StringUtils.isBlank(connectionString)) {
            queue.setConnectionString(connectionString);
        }

        String databaseName = spec.getField(SPEC_FIELD_DATABASE_NAME);
        if (!StringUtils.isBlank(databaseName)) {
            queue.setDatabaseName(databaseName);
        }

        String collectionName = spec.getField(SPEC_FIELD_COLLECTION_NAME);
        if (!StringUtils.isBlank(collectionName)) {
            queue.setCollectionName(collectionName);
        }

        super.initQueue(queue, spec);
    }
}

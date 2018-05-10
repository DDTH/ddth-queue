package com.github.ddth.queue.impl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.commons.redis.JedisConnector;

/**
 * Base factory to create {@link BaseRedisQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.2.6
 */
public abstract class BaseRedisQueueFactory<T extends RedisQueue<ID, DATA>, ID, DATA>
        extends AbstractQueueFactory<T, ID, DATA> {

    private final Logger LOGGER = LoggerFactory.getLogger(BaseRedisQueueFactory.class);

    public final static String SPEC_FIELD_HASH_NAME = "hash_name";
    public final static String SPEC_FIELD_LIST_NAME = "list_name";
    public final static String SPEC_FIELD_SORTED_SET_NAME = "sorted_set_name";
    public final static String SPEC_FIELD_HOST_AND_PORT = "host_and_port";
    public final static String SPEC_FIELD_PASSWORD = "password";

    private JedisConnector jedisConnector;
    /**
     * Flag to mark if the Redis resource (e.g. Redis client pool) is created
     * and handled by the factory.
     */
    protected boolean myOwnRedis = false;

    private String defaultPassword;
    private String defaultHashName = RedisQueue.DEFAULT_HASH_NAME,
            defaultListName = RedisQueue.DEFAULT_LIST_NAME,
            defaultSortedSetName = RedisQueue.DEFAULT_SORTED_SET_NAME;

    /**
     * @return
     * @since 0.6.2.5
     */
    protected JedisConnector getJedisConnector() {
        return jedisConnector;
    }

    /**
     * @param jedisConnector
     * @return
     * @since 0.6.2.5
     */
    public BaseRedisQueueFactory<T, ID, DATA> setJedisConnector(JedisConnector jedisConnector) {
        if (myOwnRedis && this.jedisConnector != null) {
            this.jedisConnector.close();
        }
        this.jedisConnector = jedisConnector;
        myOwnRedis = false;
        return this;
    }

    /**
     * Redis' password.
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultPassword() {
        return defaultPassword;
    }

    /**
     * Redis' password.
     * 
     * @param defaultPassword
     * @since 0.6.2
     */
    public BaseRedisQueueFactory<T, ID, DATA> setDefaultPassword(String defaultPassword) {
        this.defaultPassword = defaultPassword;
        return this;
    }

    /**
     * Name of the Redis hash to store queue messages.
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultHashName() {
        return defaultHashName;
    }

    /**
     * Name of the Redis hash to store queue messages.
     * 
     * @param defaultHashName
     * @since 0.6.2
     */
    public BaseRedisQueueFactory<T, ID, DATA> setDefaultHashName(String defaultHashName) {
        this.defaultHashName = defaultHashName;
        return this;
    }

    /**
     * Name of the Redis list to store queue message ids.
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultListName() {
        return defaultListName;
    }

    /**
     * Name of the Redis list to store queue message ids.
     * 
     * @param defaultListName
     * @since 0.6.2
     */
    public BaseRedisQueueFactory<T, ID, DATA> setDefaultListName(String defaultListName) {
        this.defaultListName = defaultListName;
        return this;
    }

    /**
     * Name of the Redis sorted-set to store ephemeral message ids.
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultSortedSetName() {
        return defaultSortedSetName;
    }

    /**
     * Name of the Redis sorted-set to store ephemeral message ids.
     * 
     * @param defaultSortedSetName
     * @since 0.6.2
     */
    public BaseRedisQueueFactory<T, ID, DATA> setDefaultSortedSetName(String defaultSortedSetName) {
        this.defaultSortedSetName = defaultSortedSetName;
        return this;
    }

    /**
     * Build a {@link JedisConnector} instance for my own use.
     * 
     * @return
     * @since 0.6.2.6
     */
    protected abstract JedisConnector buildJedisConnector();

    /**
     * Init method.
     * 
     * @return
     * @throws Exception
     * @since 0.6.2.6
     */
    public BaseRedisQueueFactory<T, ID, DATA> init() {
        if (jedisConnector == null) {
            jedisConnector = buildJedisConnector();
            myOwnRedis = jedisConnector != null;
        }

        super.init();

        return this;
    }

    /**
     * Destroy method.
     * 
     * @since 0.6.2.6
     */
    public void destroy() {
        try {
            super.destroy();
        } finally {
            if (jedisConnector != null && myOwnRedis) {
                try {
                    jedisConnector.destroy();
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                } finally {
                    jedisConnector = null;
                }
            }
        }
    }
}

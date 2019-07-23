package com.github.ddth.queue.impl;

import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.queue.internal.utils.RedisUtils;

/**
 * Base factory to create {@link BaseRedisQueue} instances.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.2.6
 */
public abstract class BaseRedisQueueFactory<T extends RedisQueue<ID, DATA>, ID, DATA>
        extends AbstractQueueFactory<T, ID, DATA> {
    public final static String SPEC_FIELD_HASH_NAME = "hash_name";
    public final static String SPEC_FIELD_LIST_NAME = "list_name";
    public final static String SPEC_FIELD_SORTED_SET_NAME = "sorted_set_name";
    public final static String SPEC_FIELD_HOST_AND_PORT = "host_and_port";
    public final static String SPEC_FIELD_PASSWORD = "password";

    private JedisConnector defaultJedisConnector;
    /**
     * Flag to mark if the Redis resource (e.g. Redis client pool) is created
     * and handled by the factory.
     */
    protected boolean myOwnRedis = false;

    private String defaultPassword;
    private String defaultHashName = RedisQueue.DEFAULT_HASH_NAME, defaultListName = RedisQueue.DEFAULT_LIST_NAME, defaultSortedSetName = RedisQueue.DEFAULT_SORTED_SET_NAME;

    /**
     * If all {@link BaseRedisQueue} instances are connecting to one Redis
     * server or cluster, it's a good idea to pre-create a
     * {@link JedisConnector} instance and share it amongst
     * {@link BaseRedisQueue} instances created from this factory by assigning
     * it to {@link #defaultJedisConnector} (see
     * {@link #setDefaultJedisConnector(JedisConnector)}).
     *
     * @return
     * @since 0.6.2.5
     */
    protected JedisConnector getDefaultJedisConnector() {
        return defaultJedisConnector;
    }

    /**
     * If all {@link BaseRedisQueue} instances are connecting to one Redis
     * server or cluster, it's a good idea to pre-create a
     * {@link JedisConnector} instance and share it amongst
     * {@link BaseRedisQueue} instances created from this factory by assigning
     * it to {@link #defaultJedisConnector} (see
     * {@link #setDefaultJedisConnector(JedisConnector)}).
     *
     * @param jedisConnector
     * @param setMyOwnRedis
     * @return
     * @since 0.7.1
     */
    protected BaseRedisQueueFactory<T, ID, DATA> setDefaultJedisConnector(JedisConnector jedisConnector,
            boolean setMyOwnRedis) {
        if (myOwnRedis && this.defaultJedisConnector != null) {
            this.defaultJedisConnector.destroy();
        }
        this.defaultJedisConnector = jedisConnector;
        myOwnRedis = setMyOwnRedis;
        return this;
    }

    /**
     * If all {@link BaseRedisQueue} instances are connecting to one Redis
     * server or cluster, it's a good idea to pre-create a
     * {@link JedisConnector} instance and share it amongst
     * {@link BaseRedisQueue} instances created from this factory by assigning
     * it to {@link #defaultJedisConnector} (see
     * {@link #setDefaultJedisConnector(JedisConnector)}).
     *
     * @param jedisConnector
     * @return
     * @since 0.6.2.5
     */
    public BaseRedisQueueFactory<T, ID, DATA> setDefaultJedisConnector(JedisConnector jedisConnector) {
        return setDefaultJedisConnector(jedisConnector, false);
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
     * Destroy method.
     *
     * @since 0.6.2.6
     */
    public void destroy() {
        try {
            super.destroy();
        } finally {
            defaultJedisConnector = RedisUtils.closeJedisConnector(defaultJedisConnector, myOwnRedis);
        }
    }
}

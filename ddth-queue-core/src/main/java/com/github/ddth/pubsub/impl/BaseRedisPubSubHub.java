package com.github.ddth.pubsub.impl;

import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.internal.utils.RedisUtils;

/**
 * Base Redis implementation of {@link IPubSubHub}.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public abstract class BaseRedisPubSubHub<ID, DATA> extends AbstractPubSubHub<ID, DATA> {
    public final static String DEFAULT_PASSWORD = null;
    private String redisPassword = DEFAULT_PASSWORD;
    private JedisConnector jedisConnector;

    /**
     * Flag to mark if the Redis resource (e.g. Redis client pool) is created
     * and handled by this object.
     */
    protected boolean myOwnRedis = true;

    /**
     * Get the current {@link JedisConnector} used by this pubsub-hub.
     *
     * @return
     */
    public JedisConnector getJedisConnector() {
        return jedisConnector;
    }

    /**
     * Set the external {@link JedisConnector} to be used by this pubsub-hub.
     *
     * @param jedisConnector
     * @return
     */
    public BaseRedisPubSubHub<ID, DATA> setJedisConnector(JedisConnector jedisConnector) {
        return setJedisConnector(jedisConnector, false);
    }

    /**
     * Set the external {@link JedisConnector} to be used by this pubsub-hub.
     *
     * @param jedisConnector
     * @param setMyOwnRedis  mark the flag {@link #myOwnRedis}
     * @return
     */
    protected BaseRedisPubSubHub<ID, DATA> setJedisConnector(JedisConnector jedisConnector, boolean setMyOwnRedis) {
        if (myOwnRedis && this.jedisConnector != null) {
            this.jedisConnector.destroy();
        }
        this.jedisConnector = jedisConnector;
        myOwnRedis = setMyOwnRedis;
        return this;
    }

    /**
     * Password to connect to Redis.
     *
     * @return
     */
    public String getRedisPassword() {
        return redisPassword;
    }

    /**
     * Password to connect to Redis.
     *
     * @param redisPassword
     * @return
     */
    public BaseRedisPubSubHub<ID, DATA> setRedisPassword(String redisPassword) {
        this.redisPassword = redisPassword;
        return this;
    }

    /*----------------------------------------------------------------------*/

    /**
     * Build a {@link JedisConnector} instance for my own use.
     *
     * @return
     */
    protected abstract JedisConnector buildJedisConnector();

    /**
     * Init method.
     *
     * @return
     * @throws Exception
     */
    public BaseRedisPubSubHub<ID, DATA> init() {
        if (jedisConnector == null) {
            jedisConnector = buildJedisConnector();
            myOwnRedis = jedisConnector != null;
        }

        super.init();

        if (jedisConnector == null) {
            throw new IllegalStateException("Jedis connector is null.");
        }

        return this;
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        try {
            super.destroy();
        } finally {
            jedisConnector = RedisUtils.closeJedisConnector(jedisConnector, myOwnRedis);
        }
    }
}

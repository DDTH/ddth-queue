package com.github.ddth.queue.impl;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.queue.QueueSpec;

import redis.clients.jedis.JedisPool;

/**
 * Factory to create {@link RedisQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public abstract class RedisQueueFactory<T extends RedisQueue<ID, DATA>, ID, DATA>
        extends AbstractQueueFactory<T, ID, DATA> {

    public final static String SPEC_FIELD_HASH_NAME = "hash_name";
    public final static String SPEC_FIELD_LIST_NAME = "list_name";
    public final static String SPEC_FIELD_SORTED_SET_NAME = "sorted_set_name";
    public final static String SPEC_FIELD_HOST_AND_PORT = "host_and_port";
    public final static String SPEC_FIELD_PASSWORD = "password";

    private JedisPool defaultJedisPool;
    private String defaultHostAndPort = RedisQueue.DEFAULT_HOST_AND_PORT,
            defaultPassword = RedisQueue.DEFAULT_PASSWORD;
    private String defaultHashName = RedisQueue.DEFAULT_HASH_NAME,
            defaultListName = RedisQueue.DEFAULT_LIST_NAME,
            defaultSortedSetName = RedisQueue.DEFAULT_SORTED_SET_NAME;

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public JedisPool getDefaultJedisPool() {
        return defaultJedisPool;
    }

    /**
     * 
     * @param defaultJedisPool
     * @since 0.6.2
     */
    public void setDefaultJedisPool(JedisPool defaultJedisPool) {
        this.defaultJedisPool = defaultJedisPool;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultHostAndPort() {
        return defaultHostAndPort;
    }

    public void setDefaultHostAndPort(String defaultHostAndPort) {
        this.defaultHostAndPort = defaultHostAndPort;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultPassword() {
        return defaultPassword;
    }

    public void setDefaultPassword(String defaultPassword) {
        this.defaultPassword = defaultPassword;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultHashName() {
        return defaultHashName;
    }

    /**
     * 
     * @param defaultHashName
     * @since 0.6.2
     */
    public void setDefaultHashName(String defaultHashName) {
        this.defaultHashName = defaultHashName;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultListName() {
        return defaultListName;
    }

    /**
     * 
     * @param defaultListName
     * @since 0.6.2
     */
    public void setDefaultListName(String defaultListName) {
        this.defaultListName = defaultListName;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultSortedSetName() {
        return defaultSortedSetName;
    }

    /**
     * 
     * @param defaultSortedSetName
     * @since 0.6.2
     */
    public void setDefaultSortedSetName(String defaultSortedSetName) {
        this.defaultSortedSetName = defaultSortedSetName;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void initQueue(T queue, QueueSpec spec) {
        super.initQueue(queue, spec);

        queue.setEphemeralDisabled(getDefaultEphemeralDisabled())
                .setEphemeralMaxSize(getDefaultEphemeralMaxSize());
        Boolean ephemeralDisabled = spec.getField(QueueSpec.FIELD_EPHEMERAL_DISABLED,
                Boolean.class);
        if (ephemeralDisabled != null) {
            queue.setEphemeralDisabled(ephemeralDisabled.booleanValue());
        }
        Integer maxEphemeralSize = spec.getField(QueueSpec.FIELD_EPHEMERAL_MAX_SIZE, Integer.class);
        if (maxEphemeralSize != null) {
            queue.setEphemeralMaxSize(maxEphemeralSize.intValue());
        }

        queue.setJedisPool(defaultJedisPool).setRedisHostAndPort(defaultHostAndPort)
                .setRedisPassword(defaultPassword);
        String redisHostAndPort = spec.getField(SPEC_FIELD_HOST_AND_PORT);
        if (!StringUtils.isBlank(redisHostAndPort)) {
            queue.setRedisHostAndPort(redisHostAndPort);
        }
        String redisPassword = spec.getField(SPEC_FIELD_PASSWORD);
        if (!StringUtils.isBlank(redisPassword)) {
            queue.setRedisPassword(redisPassword);
        }

        queue.setRedisHashName(defaultHashName).setRedisListName(defaultListName)
                .setRedisSortedSetName(defaultSortedSetName);
        String redisHashName = spec.getField(SPEC_FIELD_HASH_NAME);
        String redisListName = spec.getField(SPEC_FIELD_LIST_NAME);
        String redisSortedSetName = spec.getField(SPEC_FIELD_SORTED_SET_NAME);
        if (!StringUtils.isBlank(redisHashName) && !StringUtils.isBlank(redisListName)
                && !StringUtils.isBlank(redisSortedSetName)) {
            queue.setRedisHashName(redisHashName);
            queue.setRedisListName(redisListName);
            queue.setRedisSortedSetName(redisSortedSetName);
        } else if (!StringUtils.isBlank(redisHashName) || !StringUtils.isBlank(redisListName)
                || !StringUtils.isBlank(redisSortedSetName)) {
            throw new IllegalArgumentException("Either supply all parameters ["
                    + SPEC_FIELD_HASH_NAME + "], [" + SPEC_FIELD_LIST_NAME + "] and ["
                    + SPEC_FIELD_SORTED_SET_NAME + "] or none at all!");
        }

        queue.init();
    }

}

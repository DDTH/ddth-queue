package com.github.ddth.queue.impl;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.commons.redis.JedisUtils;
import com.github.ddth.queue.QueueSpec;

/**
 * Factory to create {@link RedisQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public abstract class RedisQueueFactory<T extends RedisQueue<ID, DATA>, ID, DATA>
        extends BaseRedisQueueFactory<T, ID, DATA> {

    private String defaultHostAndPort = RedisQueue.DEFAULT_HOST_AND_PORT;

    /**
     * Redis' host and port scheme format {@code host:port}.
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultHostAndPort() {
        return defaultHostAndPort;
    }

    /**
     * Redis' host and port scheme format {@code host:port}.
     * 
     * @param defaultHostAndPort
     * @since 0.6.2
     */
    public void setDefaultHostAndPort(String defaultHostAndPort) {
        this.defaultHostAndPort = defaultHostAndPort;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected JedisConnector buildJedisConnector() {
        JedisConnector jedisConnector = new JedisConnector();
        jedisConnector.setJedisPoolConfig(JedisUtils.defaultJedisPoolConfig())
                .setRedisPassword(getDefaultPassword())
                .setRedisHostsAndPorts(getDefaultHostAndPort()).init();
        return jedisConnector;
    }

    /**
     * {@inheritDoc}
     * 
     * @throws Exception
     */
    @Override
    protected void initQueue(T queue, QueueSpec spec) throws Exception {
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

        queue.setRedisHostAndPort(getDefaultHostAndPort()).setRedisPassword(getDefaultPassword())
                .setJedisConnector(getJedisConnector());
        String redisHostAndPort = spec.getField(SPEC_FIELD_HOST_AND_PORT);
        if (!StringUtils.isBlank(redisHostAndPort)) {
            queue.setRedisHostAndPort(redisHostAndPort);
        }
        String redisPassword = spec.getField(SPEC_FIELD_PASSWORD);
        if (!StringUtils.isBlank(redisPassword)) {
            queue.setRedisPassword(redisPassword);
        }

        queue.setRedisHashName(getDefaultHashName()).setRedisListName(getDefaultListName())
                .setRedisSortedSetName(getDefaultSortedSetName());
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

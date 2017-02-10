package com.github.ddth.queue.impl;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.queue.QueueSpec;

/**
 * Factory to create {@link RedisQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public abstract class RedisQueueFactory<T extends RedisQueue> extends AbstractQueueFactory<T> {

    public final static String SPEC_FIELD_HASH_NAME = "hash_name";
    public final static String SPEC_FIELD_LIST_NAME = "list_name";
    public final static String SPEC_FIELD_SORTED_SET_NAME = "sorted_set_name";
    public final static String SPEC_FIELD_HOST_AND_PORT = "host_and_port";

    /**
     * {@inheritDoc}
     */
    @Override
    protected void initQueue(T queue, QueueSpec spec) {
        Boolean ephemeralDisabled = spec.getField(QueueSpec.FIELD_EPHEMERAL_DISABLED,
                Boolean.class);
        if (ephemeralDisabled != null) {
            queue.setEphemeralDisabled(ephemeralDisabled.booleanValue());
        }
        Integer maxEphemeralSize = spec.getField(QueueSpec.FIELD_EPHEMERAL_MAX_SIZE, Integer.class);
        if (maxEphemeralSize != null) {
            queue.setEphemeralMaxSize(maxEphemeralSize.intValue());
        }

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

        String redisHostAndPort = spec.getField(SPEC_FIELD_HOST_AND_PORT);
        if (!StringUtils.isBlank(redisHostAndPort)) {
            queue.setRedisHostAndPort(redisHostAndPort);
        }

        queue.init();
    }

}

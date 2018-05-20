package com.github.ddth.queue.impl;

import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.commons.redis.JedisUtils;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;
import com.github.ddth.queue.utils.QueueUtils;

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCommands;
import redis.clients.jedis.Protocol;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

/**
 * Redis implementation of {@link IQueue}.
 * 
 * <p>
 * Implementation:
 * <ul>
 * <li>A hash to store message, format {queue_id => message}. See
 * {@link #setRedisHashName(String)}.
 * <li>A list to act as a queue of message's queue_id. See
 * {@link #setRedisListName(String)}.
 * <li>A sorted set to act as ephemeral storage of message's queue_id, score is
 * message's timestamp. See {@link #setRedisSortedSetName(String)}.</li>
 * </ul>
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.1
 */
public abstract class RedisQueue<ID, DATA> extends BaseRedisQueue<ID, DATA> {

    public final static String DEFAULT_HOST_AND_PORT = Protocol.DEFAULT_HOST + ":"
            + Protocol.DEFAULT_PORT;

    private String redisHostAndPort = DEFAULT_HOST_AND_PORT, redisPassword = DEFAULT_PASSWORD;

    /**
     * Redis' host and port scheme (format {@code host:port}).
     *
     * @return
     */
    public String getRedisHostAndPort() {
        return redisHostAndPort;
    }

    /**
     * Set Redis' host and port scheme (format {@code host:port}).
     *
     * @param redisHostAndPort
     * @return
     */
    public RedisQueue<ID, DATA> setRedisHostAndPort(String redisHostAndPort) {
        this.redisHostAndPort = redisHostAndPort;
        return this;
    }

    /*----------------------------------------------------------------------*/
    /**
     * {@inheritDoc}
     */
    @Override
    protected JedisConnector buildJedisConnector() {
        JedisConnector jedisConnector = new JedisConnector();
        jedisConnector.setJedisPoolConfig(JedisUtils.defaultJedisPoolConfig())
                .setRedisHostsAndPorts(redisHostAndPort).setRedisPassword(redisPassword).init();
        return jedisConnector;
    }

    /*----------------------------------------------------------------------*/
    /**
     * {@inheritDoc}
     */
    @Override
    protected JedisCommands getJedisCommands() {
        return getJedisConnector().getJedis();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected BinaryJedisCommands getBinaryJedisCommands() {
        return getJedisConnector().getJedis();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void closeJedisCommands(JedisCommands jedisCommands) {
        if (jedisCommands instanceof Jedis) {
            ((Jedis) jedisCommands).close();
        } else
            throw new IllegalArgumentException("Argument is not of type [" + Jedis.class + "]!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void closeJedisCommands(BinaryJedisCommands jedisCommands) {
        if (jedisCommands instanceof Jedis) {
            ((Jedis) jedisCommands).close();
        } else
            throw new IllegalArgumentException("Argument is not of type [" + Jedis.class + "]!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean remove(IQueueMessage<ID, DATA> msg) {
        if (isEphemeralDisabled()) {
            return true;
        }
        try (Jedis jedis = getJedisConnector().getJedis()) {
            Transaction jt = jedis.multi();

            byte[] field = msg.getId().toString().getBytes(QueueUtils.UTF8);
            Response<Long> response = jt.hdel(getRedisHashNameAsBytes(), field);
            jt.zrem(getRedisSortedSetNameAsBytes(), field);

            jt.exec();
            Long value = response.get();
            return value != null && value.longValue() > 1;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean storeNew(IQueueMessage<ID, DATA> msg) {
        try (Jedis jedis = getJedisConnector().getJedis()) {
            Transaction jt = jedis.multi();

            byte[] field = msg.getId().toString().getBytes(QueueUtils.UTF8);
            byte[] data = serialize(msg);
            jt.hset(getRedisHashNameAsBytes(), field, data);
            jt.rpush(getRedisListNameAsBytes(), field);

            jt.exec();
            return true;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean storeOld(IQueueMessage<ID, DATA> msg) {
        try (Jedis jedis = getJedisConnector().getJedis()) {
            Transaction jt = jedis.multi();

            byte[] field = msg.getId().toString().getBytes(QueueUtils.UTF8);
            byte[] data = serialize(msg);
            jt.hset(getRedisHashNameAsBytes(), field, data);
            jt.rpush(getRedisListNameAsBytes(), field);
            jt.zrem(getRedisSortedSetNameAsBytes(), field);

            jt.exec();
            return true;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @throws QueueException.EphemeralIsFull
     *             if the ephemeral storage is full
     */
    @Override
    public IQueueMessage<ID, DATA> take() throws QueueException.EphemeralIsFull {
        if (!isEphemeralDisabled()) {
            int ephemeralMaxSize = getEphemeralMaxSize();
            if (ephemeralMaxSize > 0 && ephemeralSize() >= ephemeralMaxSize) {
                throw new QueueException.EphemeralIsFull(ephemeralMaxSize);
            }
        }
        try (Jedis jedis = getJedisConnector().getJedis()) {
            long now = System.currentTimeMillis();
            Object response = jedis.eval(getScriptTake(), 0, String.valueOf(now));
            if (response == null) {
                return null;
            }
            return deserialize(response instanceof byte[] ? (byte[]) response
                    : response.toString().getBytes(QueueUtils.UTF8));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean moveFromEphemeralToQueueStorage(IQueueMessage<ID, DATA> msg) {
        if (isEphemeralDisabled()) {
            return true;
        }
        try (Jedis jedis = getJedisConnector().getJedis()) {
            Object response = jedis.eval(getScriptMove(), 0, msg.getId().toString());
            return response != null && "1".equals(response.toString());
        }
    }

}

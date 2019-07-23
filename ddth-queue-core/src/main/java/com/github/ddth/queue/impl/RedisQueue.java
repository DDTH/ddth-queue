package com.github.ddth.queue.impl;

import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.commons.redis.JedisUtils;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;
import redis.clients.jedis.*;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

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
 * <p>Features:</p>
 * <ul>
 * <li>Queue-size support: yes</li>
 * <li>Ephemeral storage support: yes</li>
 * <li>Ephemeral-size support: yes</li>
 * </ul>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.1
 */
public abstract class RedisQueue<ID, DATA> extends BaseRedisQueue<ID, DATA> {
    public final static String DEFAULT_HOST_AND_PORT = Protocol.DEFAULT_HOST + ":" + Protocol.DEFAULT_PORT;
    private String redisHostAndPort = DEFAULT_HOST_AND_PORT;

    /**
     * Redis host and port scheme (format {@code host:port}).
     *
     * @return
     */
    public String getRedisHostAndPort() {
        return redisHostAndPort;
    }

    /**
     * Redis host and port scheme (format {@code host:port}).
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
                .setRedisHostsAndPorts(getRedisHostAndPort()).setRedisPassword(getRedisPassword()).init();
        return jedisConnector;
    }

    /*----------------------------------------------------------------------*/

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
    protected void closeJedisCommands(BinaryJedisCommands jedisCommands) {
        if (jedisCommands instanceof Jedis) {
            ((Jedis) jedisCommands).close();
        } else
            throw new IllegalArgumentException("Argument is not of type [" + Jedis.class + "]!");
    }

    private boolean doExecTx(Transaction jt, Response<?>... responses) {
        jt.exec();
        if (responses != null) {
            for (Response<?> response : responses) {
                if (response == null) {
                    continue;
                }
                Object value = response.get();
                if (value == null || (value instanceof Number && ((Number) value).longValue() < 1)) {
                    return false;
                }
            }
        }
        return true;
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
            try (Transaction jt = jedis.multi()) {
                byte[] field = msg.getId().toString().getBytes(StandardCharsets.UTF_8);
                Response<?>[] importantResponses = new Response[] { jt.hdel(getRedisHashNameAsBytes(), field),
                        isEphemeralDisabled() ? null : jt.zrem(getRedisSortedSetNameAsBytes(), field) };
                return doExecTx(jt, importantResponses);
            } catch (IOException e) {
                throw new QueueException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean storeNew(IQueueMessage<ID, DATA> msg) {
        try (Jedis jedis = getJedisConnector().getJedis()) {
            try (Transaction jt = jedis.multi()) {
                byte[] field = msg.getId().toString().getBytes(StandardCharsets.UTF_8);
                byte[] data = serialize(msg);
                /* hset may return 0 if hash already existed. So we only care about the response of rpush */
                jt.hset(getRedisHashNameAsBytes(), field, data);
                Response<?>[] importantResponses = new Response[] { jt.rpush(getRedisListNameAsBytes(), field) };
                return doExecTx(jt, importantResponses);
            } catch (IOException e) {
                throw new QueueException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean storeOld(IQueueMessage<ID, DATA> msg) {
        try (Jedis jedis = getJedisConnector().getJedis()) {
            try (Transaction jt = jedis.multi()) {
                byte[] field = msg.getId().toString().getBytes(StandardCharsets.UTF_8);
                byte[] data = serialize(msg);
                /* zrem and hset may return 0. So we only care about the response of rpush */
                if (!isEphemeralDisabled()) {
                    jt.zrem(getRedisSortedSetNameAsBytes(), field);
                }
                jt.hset(getRedisHashNameAsBytes(), field, data);
                Response<?>[] importantResponses = new Response[] { jt.rpush(getRedisListNameAsBytes(), field) };
                return doExecTx(jt, importantResponses);
            } catch (IOException e) {
                throw new QueueException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws QueueException.EphemeralIsFull if the ephemeral storage is full
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
            Object response = jedis.eval(getScriptTakeAsBytes(), 0, String.valueOf(now).getBytes(StandardCharsets.UTF_8));
            return response == null ?
                    null :
                    deserialize(response instanceof byte[] ?
                            (byte[]) response :
                            response.toString().getBytes(StandardCharsets.UTF_8));
        }
    }
}

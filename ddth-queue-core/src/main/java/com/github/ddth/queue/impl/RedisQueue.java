package com.github.ddth.queue.impl;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;
import com.github.ddth.queue.utils.QueueUtils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
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
public abstract class RedisQueue extends AbstractEphemeralSupportQueue {

    private JedisPool jedisPool;
    private boolean myOwnJedisPool = true;
    private String redisHostAndPort = "localhost:6379";

    private String _redisHashName = "queue_h";
    private byte[] redisHashName = _redisHashName.getBytes(QueueUtils.UTF8);

    private String _redisListName = "queue_l";
    private byte[] redisListName = _redisListName.getBytes(QueueUtils.UTF8);

    private String _redisSortedSetName = "queue_s";
    private byte[] redisSortedSetName = _redisSortedSetName.getBytes(QueueUtils.UTF8);

    /**
     * Redis' host and port scheme (format {@code host:port}).
     * 
     * @return
     */
    public String getRedisHostAndPort() {
        return redisHostAndPort;
    }

    /**
     * Sets Redis' host and port scheme (format {@code host:port}).
     * 
     * @param redisHostAndPort
     * @return
     */
    public RedisQueue setRedisHostAndPort(String redisHostAndPort) {
        this.redisHostAndPort = redisHostAndPort;
        return this;
    }

    public String getRedisHashName() {
        return _redisHashName;
    }

    public RedisQueue setRedisHashName(String redisHashName) {
        _redisHashName = redisHashName;
        this.redisHashName = _redisHashName.getBytes(QueueUtils.UTF8);
        return this;
    }

    public String getRedisListName() {
        return _redisListName;
    }

    public RedisQueue setRedisListName(String redisListName) {
        _redisListName = redisListName;
        this.redisListName = _redisListName.getBytes(QueueUtils.UTF8);
        return this;
    }

    public String getRedisSortedSetName() {
        return _redisSortedSetName;
    }

    public RedisQueue setRedisSortedSetName(String redisSortedSetName) {
        _redisSortedSetName = redisSortedSetName;
        this.redisSortedSetName = _redisSortedSetName.getBytes(QueueUtils.UTF8);
        return this;
    }

    protected JedisPool getJedisPool() {
        return jedisPool;
    }

    public RedisQueue setJedisPool(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
        myOwnJedisPool = false;
        return this;
    }

    /*----------------------------------------------------------------------*/
    /**
     * LUA script to take a message out of queue.
     */
    private String SCRIPT_TAKE;

    /**
     * LUA script to move a message from ephemeral storage to queue storage.
     */
    private String SCRIPT_MOVE;

    /**
     * Init method.
     * 
     * @return
     */
    public RedisQueue init() {
        if (jedisPool == null) {
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(32);
            poolConfig.setMinIdle(1);
            poolConfig.setMaxIdle(16);
            poolConfig.setMaxWaitMillis(10000);
            // poolConfig.setTestOnBorrow(true);
            poolConfig.setTestWhileIdle(true);

            String[] tokens = redisHostAndPort.split(":");
            String redisHost = tokens.length > 0 ? tokens[0] : "localhost";
            int redisPort = tokens.length > 1 ? Integer.parseInt(tokens[1]) : 6379;
            jedisPool = new JedisPool(poolConfig, redisHost, redisPort);
            myOwnJedisPool = true;
        }

        if (isEphemeralDisabled()) {
            /*
             * Script details (ephemeral is disabled): lpop qId from the List
             * and hget message's content from the Hash and remove it from the
             * Hash, atomically. Finally, the message's content is returned.
             * 
             * Script's first argument (ARGV[1]) is the qId's associated
             * timestamp to be used as score value for the SortedSet entry.
             */
            SCRIPT_TAKE = "local qid=redis.call(\"lpop\",\"{0}\"); if qid then "
                    + "local qcontent=redis.call(\"hget\", \"{2}\", qid); "
                    + "redis.call(\"hdel\", \"{2}\", qid); return qcontent "
                    + "else return nil end";
        } else {
            /*
             * Script details (ephemeral is enabled): lpop qId from the List and
             * zadd {ARGV[1]:qId} to the SortedSet and hget message's content
             * from the Hash, atomically. Finally, the message's content is
             * returned.
             */
            SCRIPT_TAKE = "local qid=redis.call(\"lpop\",\"{0}\"); if qid then "
                    + "redis.call(\"zadd\", \"{1}\", ARGV[1], qid); return redis.call(\"hget\", \"{2}\", qid) "
                    + "else return nil end";
        }
        SCRIPT_TAKE = MessageFormat.format(SCRIPT_TAKE, _redisListName, _redisSortedSetName,
                _redisHashName);

        /*
         * Script details: remove qId from the SortedSet and rpush it to the
         * List, atomically.
         * 
         * Script's first argument (ARGV[1]) is qId.
         */
        SCRIPT_MOVE = "local result=redis.call(\"zrem\",\"{0}\",ARGV[1]); if result then "
                + "redis.call(\"rpush\", \"{1}\",  ARGV[1]); return 1; else return 0; end";
        SCRIPT_MOVE = MessageFormat.format(SCRIPT_MOVE, _redisSortedSetName, _redisListName);

        return this;
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        if (jedisPool != null && myOwnJedisPool) {
            jedisPool.destroy();
            jedisPool = null;
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.4.0
     */
    @Override
    public void close() {
        destroy();
    }

    /**
     * Serializes a queue message to store in Redis.
     * 
     * @param msg
     * @return
     */
    protected abstract byte[] serialize(IQueueMessage msg);

    /**
     * Deserilizes a queue message.
     * 
     * @param msgData
     * @return
     */
    protected abstract IQueueMessage deserialize(byte[] msgData);

    /**
     * Removes a message completely.
     * 
     * @param msg
     * @return
     */
    protected boolean remove(IQueueMessage msg) {
        if (isEphemeralDisabled()) {
            return true;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction jt = jedis.multi();

            byte[] field = msg.qId().toString().getBytes(QueueUtils.UTF8);
            Response<Long> response = jt.hdel(redisHashName, field);
            jt.zrem(redisSortedSetName, field);

            jt.exec();
            Long value = response.get();
            return value != null && value.longValue() > 1;
        }
    }

    /**
     * Stores a new message.
     * 
     * @param msg
     * @return
     */
    protected boolean storeNew(IQueueMessage msg) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction jt = jedis.multi();

            byte[] field = msg.qId().toString().getBytes(QueueUtils.UTF8);
            byte[] data = serialize(msg);
            jt.hset(redisHashName, field, data);
            jt.rpush(redisListName, field);

            jt.exec();
            return true;
        }
    }

    /**
     * Re-stores an old message (called by {@link #requeue(IQueueMessage)} or
     * {@link #requeueSilent(IQueueMessage)}.
     * 
     * @param msg
     * @return
     */
    protected boolean storeOld(IQueueMessage msg) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction jt = jedis.multi();

            byte[] field = msg.qId().toString().getBytes(QueueUtils.UTF8);
            byte[] data = serialize(msg);
            jt.hset(redisHashName, field, data);
            jt.rpush(redisListName, field);
            jt.zrem(redisSortedSetName, field);

            jt.exec();
            return true;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean queue(IQueueMessage _msg) {
        IQueueMessage msg = _msg.clone();
        Date now = new Date();
        msg.qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now);
        return storeNew(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeue(IQueueMessage _msg) {
        IQueueMessage msg = _msg.clone();
        Date now = new Date();
        msg.qIncNumRequeues().qTimestamp(now);
        return isEphemeralDisabled() ? storeNew(msg) : storeOld(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeueSilent(IQueueMessage msg) {
        return isEphemeralDisabled() ? storeNew(msg.clone()) : storeOld(msg.clone());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage msg) {
        remove(msg);
    }

    /**
     * {@inheritDoc}
     * 
     * @throws QueueException.EphemeralIsFull
     *             if the ephemeral storage is full
     */
    @Override
    public IQueueMessage take() throws QueueException.EphemeralIsFull {
        if (!isEphemeralDisabled()) {
            int ephemeralMaxSize = getEphemeralMaxSize();
            if (ephemeralMaxSize > 0 && ephemeralSize() >= ephemeralMaxSize) {
                throw new QueueException.EphemeralIsFull(ephemeralMaxSize);
            }
        }
        try (Jedis jedis = jedisPool.getResource()) {
            long now = System.currentTimeMillis();
            Object response = jedis.eval(SCRIPT_TAKE, 0, String.valueOf(now));
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
    public Collection<IQueueMessage> getOrphanMessages(long thresholdTimestampMs) {
        if (isEphemeralDisabled()) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            long now = System.currentTimeMillis();
            Collection<IQueueMessage> result = new HashSet<IQueueMessage>();
            byte[] min = "0".getBytes();
            byte[] max = String.valueOf(now - thresholdTimestampMs).getBytes();
            Set<byte[]> fields = jedis.zrangeByScore(redisSortedSetName, min, max, 0, 100);
            for (byte[] field : fields) {
                byte[] data = jedis.hget(redisHashName, field);
                IQueueMessage msg = deserialize(data);
                if (msg != null) {
                    result.add(msg);
                }
            }
            return result;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean moveFromEphemeralToQueueStorage(IQueueMessage msg) {
        if (isEphemeralDisabled()) {
            return true;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            Object response = jedis.eval(SCRIPT_MOVE, 0, msg.qId().toString());
            return response != null && "1".equals(response.toString());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        try (Jedis jedis = jedisPool.getResource()) {
            Long result = jedis.llen(redisListName);
            return result != null ? result.intValue() : 0;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        if (isEphemeralDisabled()) {
            return 0;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            Long result = jedis.zcard(redisSortedSetName);
            return result != null ? result.intValue() : 0;
        }
    }
}

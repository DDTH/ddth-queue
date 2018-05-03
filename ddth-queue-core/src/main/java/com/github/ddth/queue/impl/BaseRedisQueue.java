package com.github.ddth.queue.impl;

import java.text.MessageFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueUtils;

import redis.clients.jedis.BinaryJedisCommands;
import redis.clients.jedis.JedisCommands;

/**
 * Base Redis implementation of {@link IQueue}.
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
 * @since 0.6.2.6
 */
public abstract class BaseRedisQueue<ID, DATA> extends AbstractEphemeralSupportQueue<ID, DATA> {

    private final Logger LOGGER = LoggerFactory.getLogger(BaseRedisQueue.class);

    public final static String DEFAULT_PASSWORD = null;
    public final static String DEFAULT_HASH_NAME = "queue_h";
    public final static String DEFAULT_LIST_NAME = "queue_l";
    public final static String DEFAULT_SORTED_SET_NAME = "queue_s";

    private String redisPassword = DEFAULT_PASSWORD;
    private JedisConnector jedisConnector;
    /**
     * Flag to mark if the Redis resource (e.g. Redis client pool) is created
     * and handled by the lock instance.
     */
    protected boolean myOwnRedis = true;

    private String _redisHashName = DEFAULT_HASH_NAME;
    private byte[] redisHashName = _redisHashName.getBytes(QueueUtils.UTF8);

    private String _redisListName = DEFAULT_LIST_NAME;
    private byte[] redisListName = _redisListName.getBytes(QueueUtils.UTF8);

    private String _redisSortedSetName = DEFAULT_SORTED_SET_NAME;
    private byte[] redisSortedSetName = _redisSortedSetName.getBytes(QueueUtils.UTF8);

    /**
     * Get the current {@link JedisConnector} used by this queue.
     * 
     * @return
     */
    public JedisConnector getJedisConnector() {
        return jedisConnector;
    }

    /**
     * Set the external {@link JedisConnector} to be used by this queue.
     * 
     * @param jedisConnector
     * @return
     */
    public BaseRedisQueue<ID, DATA> setJedisConnector(JedisConnector jedisConnector) {
        if (myOwnRedis && this.jedisConnector != null) {
            this.jedisConnector.destroy();
        }
        this.jedisConnector = jedisConnector;
        myOwnRedis = false;
        return this;
    }

    /**
     * Redis' password.
     * 
     * @return
     */
    public String getRedisPassword() {
        return redisPassword;
    }

    /**
     * Redis' password.
     * 
     * @param redisPassword
     * @return
     */
    public BaseRedisQueue<ID, DATA> setRedisPassword(String redisPassword) {
        this.redisPassword = redisPassword;
        return this;
    }

    /**
     * Name of the Redis hash to store queue messages.
     * 
     * @return
     */
    public String getRedisHashName() {
        return _redisHashName;
    }

    /**
     * Name of the Redis hash to store queue messages.
     * 
     * @return
     */
    public byte[] getRedisHashNameAsBytes() {
        return redisHashName;
    }

    /**
     * Name of the Redis hash to store queue messages.
     * 
     * @param redisHashName
     * @return
     */
    public BaseRedisQueue<ID, DATA> setRedisHashName(String redisHashName) {
        _redisHashName = redisHashName;
        this.redisHashName = _redisHashName.getBytes(QueueUtils.UTF8);
        return this;
    }

    /**
     * Name of the Redis list to store queue message ids.
     * 
     * @return
     */
    public String getRedisListName() {
        return _redisListName;
    }

    /**
     * Name of the Redis list to store queue message ids.
     * 
     * @return
     */
    public byte[] getRedisListNameAsBytes() {
        return redisListName;
    }

    /**
     * Name of the Redis list to store queue message ids.
     * 
     * @param redisListName
     * @return
     */
    public BaseRedisQueue<ID, DATA> setRedisListName(String redisListName) {
        _redisListName = redisListName;
        this.redisListName = _redisListName.getBytes(QueueUtils.UTF8);
        return this;
    }

    /**
     * Name of the Redis sorted-set to store ephemeral message ids.
     * 
     * @return
     */
    public String getRedisSortedSetName() {
        return _redisSortedSetName;
    }

    /**
     * Name of the Redis sorted-set to store ephemeral message ids.
     * 
     * @return
     */
    public byte[] getRedisSortedSetNameAsBytes() {
        return redisSortedSetName;
    }

    /**
     * Name of the Redis sorted-set to store ephemeral message ids.
     * 
     * @param redisSortedSetName
     * @return
     */
    public BaseRedisQueue<ID, DATA> setRedisSortedSetName(String redisSortedSetName) {
        _redisSortedSetName = redisSortedSetName;
        this.redisSortedSetName = _redisSortedSetName.getBytes(QueueUtils.UTF8);
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
     * LUA script to take a message out of queue.
     * 
     * @return
     */
    public String getScriptTake() {
        return SCRIPT_TAKE;
    }

    /**
     * LUA script to take a message out of queue.
     * 
     * <p>
     * Normally queue implementations will build this script; no need to "set"
     * it from outside.
     * </p>
     * 
     * @param scriptTake
     * @return
     */
    public BaseRedisQueue<ID, DATA> setScriptTake(String scriptTake) {
        SCRIPT_TAKE = scriptTake;
        return this;
    }

    /**
     * LUA script to move a message from ephemeral storage to queue storage.
     * 
     * @return
     */
    public String getScriptMove() {
        return SCRIPT_MOVE;
    }

    /**
     * LUA script to move a message from ephemeral storage to queue storage.
     *
     * <p>
     * Normally queue implementations will build this script; no need to "set"
     * it from outside.
     * </p>
     * 
     * @param scriptMove
     * @return
     */
    public BaseRedisQueue<ID, DATA> setScriptMove(String scriptMove) {
        SCRIPT_MOVE = scriptMove;
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
     */
    public BaseRedisQueue<ID, DATA> init() throws Exception {
        if (jedisConnector == null) {
            jedisConnector = buildJedisConnector();
            myOwnRedis = jedisConnector != null;
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

    /**
     * Serialize a queue message to store in Redis.
     * 
     * @param msg
     * @return
     */
    protected abstract byte[] serialize(IQueueMessage<ID, DATA> msg);

    /**
     * Deserilize a queue message.
     * 
     * @param msgData
     * @return
     */
    protected abstract IQueueMessage<ID, DATA> deserialize(byte[] msgData);

    /*----------------------------------------------------------------------*/
    /**
     * Get {@link JedisCommands} instance.
     * 
     * @return
     * @since 0.6.2.6
     */
    protected abstract JedisCommands getJedisCommands();

    /**
     * Get {@link BinaryJedisCommands} instance.
     * 
     * @return
     * @since 0.6.2.6
     */
    protected abstract BinaryJedisCommands getBinaryJedisCommands();

    /**
     * Close the unused {@link JedisCommands}.
     * 
     * @param jedisCommands
     * @since 0.6.2.6
     */
    protected abstract void closeJedisCommands(JedisCommands jedisCommands);

    /**
     * Close the unused {@link BinaryJedisCommands}.
     * 
     * @param jedisCommands
     * @since 0.6.2.6
     */
    protected abstract void closeJedisCommands(BinaryJedisCommands jedisCommands);

    /**
     * Remove a message completely.
     * 
     * @param msg
     * @return
     */
    protected abstract boolean remove(IQueueMessage<ID, DATA> msg);

    /**
     * Store a new message.
     * 
     * @param msg
     * @return
     */
    protected abstract boolean storeNew(IQueueMessage<ID, DATA> msg);

    /**
     * Re-store an old message (called by {@link #requeue(IQueueMessage)} or
     * {@link #requeueSilent(IQueueMessage)}.
     * 
     * @param msg
     * @return
     */
    protected abstract boolean storeOld(IQueueMessage<ID, DATA> msg);

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean queue(IQueueMessage<ID, DATA> _msg) {
        IQueueMessage<ID, DATA> msg = _msg.clone();
        Date now = new Date();
        msg.qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now);
        return storeNew(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeue(IQueueMessage<ID, DATA> _msg) {
        IQueueMessage<ID, DATA> msg = _msg.clone();
        Date now = new Date();
        msg.qIncNumRequeues().qTimestamp(now);
        return isEphemeralDisabled() ? storeNew(msg) : storeOld(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeueSilent(IQueueMessage<ID, DATA> msg) {
        return isEphemeralDisabled() ? storeNew(msg.clone()) : storeOld(msg.clone());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage<ID, DATA> msg) {
        remove(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<IQueueMessage<ID, DATA>> getOrphanMessages(long thresholdTimestampMs) {
        if (isEphemeralDisabled()) {
            return null;
        }
        BinaryJedisCommands jc = getBinaryJedisCommands();
        try {
            long now = System.currentTimeMillis();
            Collection<IQueueMessage<ID, DATA>> result = new HashSet<>();
            byte[] min = "0".getBytes();
            byte[] max = String.valueOf(now - thresholdTimestampMs).getBytes();
            Set<byte[]> fields = jc.zrangeByScore(getRedisSortedSetNameAsBytes(), min, max, 0, 100);
            for (byte[] field : fields) {
                byte[] data = jc.hget(getRedisHashNameAsBytes(), field);
                IQueueMessage<ID, DATA> msg = deserialize(data);
                if (msg != null) {
                    result.add(msg);
                }
            }
            return result;
        } finally {
            closeJedisCommands(jc);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        BinaryJedisCommands jc = getBinaryJedisCommands();
        try {
            Long result = jc.llen(getRedisListNameAsBytes());
            return result != null ? result.intValue() : 0;
        } finally {
            closeJedisCommands(jc);
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
        BinaryJedisCommands jc = getBinaryJedisCommands();
        try {
            Long result = jc.zcard(getRedisSortedSetNameAsBytes());
            return result != null ? result.intValue() : 0;
        } finally {
            closeJedisCommands(jc);
        }
    }
}

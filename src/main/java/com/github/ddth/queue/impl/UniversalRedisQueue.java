package com.github.ddth.queue.impl;

import java.nio.charset.Charset;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.codec.binary.Base64;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import com.github.ddth.commons.utils.IdGenerator;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.UniversalQueueMessage;

/**
 * Universal Redis implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalQueueMessage}s.
 * </p>
 * 
 * <p>
 * Implementation:
 * <ul>
 * <li>A hash to store message's content, format {queue_id => message}. See
 * {@link #setRedisHashName(String)}.
 * <li>A list to act as a queue of message's queue_id. See
 * {@link #setRedisListName(String)}.
 * <li>A sorted set to act as ephemeral storage of message's queue_id, store is
 * message's timestamp. See {@link #setRedisSortedSetName(String)}.</li>
 * </ul>
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.0
 */
public class UniversalRedisQueue implements IQueue {

    // private Logger LOGGER = LoggerFactory.getLogger(RedisQueue.class);
    private IdGenerator IDGEN = IdGenerator.getInstance(IdGenerator.getMacAddr());

    private final static Charset UTF8 = Charset.forName("UTF-8");

    private JedisPool jedisPool;
    private String redisHost = "localhost";
    private int redisPort = 6379;

    private String _redisHashName = "queue_h";
    private byte[] redisHashName = _redisHashName.getBytes(UTF8);

    private String _redisListName = "queue_l";
    private byte[] redisListName = _redisListName.getBytes(UTF8);

    private String _redisSortedSetName = "queue_s";
    private byte[] redisSortedSetName = _redisSortedSetName.getBytes(UTF8);

    public String getRedisHost() {
        return redisHost;
    }

    public UniversalRedisQueue setRedisHost(String redisHost) {
        this.redisHost = redisHost;
        return this;
    }

    public int getRedisPort() {
        return redisPort;
    }

    public UniversalRedisQueue setRedisPort(int redisPort) {
        this.redisPort = redisPort;
        return this;
    }

    public String getRedisHashName() {
        return _redisHashName;
    }

    public UniversalRedisQueue setRedisHashName(String redisHashName) {
        _redisHashName = redisHashName;
        this.redisHashName = _redisHashName.getBytes(UTF8);
        return this;
    }

    public String getRedisListName() {
        return _redisListName;
    }

    public UniversalRedisQueue setRedisListName(String redisListName) {
        _redisListName = redisListName;
        this.redisListName = _redisListName.getBytes(UTF8);
        return this;
    }

    public String getRedisSortedSetName() {
        return _redisSortedSetName;
    }

    public UniversalRedisQueue setRedisSortedSetName(String redisSortedSetName) {
        _redisSortedSetName = redisSortedSetName;
        this.redisSortedSetName = _redisSortedSetName.getBytes(UTF8);
        return this;
    }

    /*----------------------------------------------------------------------*/

    private String SCRIPT_TAKE, SCRIPT_MOVE;

    /**
     * Init method.
     * 
     * @return
     */
    public UniversalRedisQueue init() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(32);
        poolConfig.setMinIdle(1);
        poolConfig.setMaxIdle(16);
        poolConfig.setMaxWaitMillis(10000);
        // poolConfig.setTestOnBorrow(true);
        poolConfig.setTestWhileIdle(true);

        jedisPool = new JedisPool(poolConfig, redisHost, redisPort);

        SCRIPT_TAKE = "local qid=redis.call(\"lpop\",\"{0}\"); if qid then "
                + "redis.call(\"zadd\", \"{1}\",  ARGV[1], qid); return redis.call(\"hget\", \"{2}\", qid) "
                + "else return nil end";
        SCRIPT_TAKE = MessageFormat.format(SCRIPT_TAKE, _redisListName, _redisSortedSetName,
                _redisHashName);

        SCRIPT_MOVE = "local result=redis.call(\"zrem\",\"{0}\",ARGV[1]); if result then "
                + "redis.call(\"rpush\", \"{1}\",  ARGV[1]); return 1; else return 0; end";
        SCRIPT_MOVE = MessageFormat.format(SCRIPT_MOVE, _redisSortedSetName, _redisListName);

        return this;
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        if (jedisPool != null) {
            jedisPool.destroy();
        }
    }

    private static byte[] serialize(UniversalQueueMessage msg) {
        if (msg == null) {
            return null;
        }
        Map<String, Object> dataMap = msg.toMap();
        byte[] content = msg.content();
        String contentStr = content != null ? Base64.encodeBase64String(content) : null;
        dataMap.put(UniversalQueueMessage.FIELD_CONTENT, contentStr);
        return SerializationUtils.toJsonString(dataMap).getBytes(UTF8);
    }

    @SuppressWarnings("unchecked")
    private static UniversalQueueMessage deserialize(Object response) {
        if (response == null) {
            return null;
        }
        String dataStr = (response instanceof byte[]) ? new String((byte[]) response, UTF8)
                : response.toString();
        Map<String, Object> dataMap = SerializationUtils.fromJsonString(dataStr, Map.class);
        Object content = dataMap.get(UniversalQueueMessage.FIELD_CONTENT);
        byte[] contentData = content == null ? null : (content instanceof byte[] ? (byte[]) content
                : Base64.decodeBase64(content.toString()));
        dataMap.put(UniversalQueueMessage.FIELD_CONTENT, contentData);
        UniversalQueueMessage msg = new UniversalQueueMessage();
        msg.fromMap(dataMap);
        return msg;
    }

    /**
     * Removes a message completely.
     * 
     * @param msg
     * @return
     */
    private boolean remove(UniversalQueueMessage msg) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction jt = jedis.multi();

            byte[] field = String.valueOf(msg.qId().longValue()).getBytes();
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
    private boolean storeNew(UniversalQueueMessage msg) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction jt = jedis.multi();

            byte[] field = String.valueOf(msg.qId().longValue()).getBytes();
            byte[] data = serialize(msg);
            jt.hset(redisHashName, field, data);
            jt.rpush(redisListName, field);

            jt.exec();
            return true;
        }
    }

    /**
     * Stores an old message (called by {@link #requeue(IQueueMessage)} or
     * {@link #requeueSilent(IQueueMessage)}.
     * 
     * @param msg
     * @return
     */
    private boolean storeOld(UniversalQueueMessage msg) {
        try (Jedis jedis = jedisPool.getResource()) {
            Transaction jt = jedis.multi();

            byte[] field = String.valueOf(msg.qId().longValue()).getBytes();
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
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }

        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        Date now = new Date();
        long qId = IDGEN.generateId64();
        msg.qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now).qId(qId);
        return storeNew(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeue(IQueueMessage _msg) {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }

        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        Date now = new Date();
        msg.qIncNumRequeues().qTimestamp(now);
        return storeOld(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeueSilent(IQueueMessage _msg) {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }

        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        return storeOld(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage _msg) {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }

        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        remove(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalQueueMessage take() {
        try (Jedis jedis = jedisPool.getResource()) {
            long timestamp = System.currentTimeMillis();
            Object response = jedis.eval(SCRIPT_TAKE, 0, String.valueOf(timestamp));
            return deserialize(response);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @param thresholdTimestampMs
     * @return
     */
    @Override
    public Collection<IQueueMessage> getOrphanMessages(long thresholdTimestampMs) {
        try (Jedis jedis = jedisPool.getResource()) {
            Collection<IQueueMessage> result = new HashSet<IQueueMessage>();

            byte[] min = "0".getBytes();
            byte[] max = String.valueOf(thresholdTimestampMs).getBytes();
            Set<byte[]> fields = jedis.zrangeByScore(redisSortedSetName, min, max, 0, 100);
            for (byte[] field : fields) {
                byte[] data = jedis.hget(redisHashName, field);
                UniversalQueueMessage msg = deserialize(data);
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
    public boolean moveFromEphemeralToQueueStorage(IQueueMessage _msg) {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }

        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        try (Jedis jedis = jedisPool.getResource()) {
            Object response = jedis.eval(SCRIPT_MOVE, 0, String.valueOf(msg.qId()));
            return response != null && "1".equals(response.toString());
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        try (Jedis jedis = jedisPool.getResource()) {
            Long result = jedis.hlen(redisHashName);
            return result != null ? result.intValue() : 0;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        try (Jedis jedis = jedisPool.getResource()) {
            Long result = jedis.zcard(redisSortedSetName);
            return result != null ? result.intValue() : 0;
        }
    }
}

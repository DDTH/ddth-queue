package com.github.ddth.queue.test.universal.idstr.redis;

import com.github.ddth.queue.impl.universal.idstr.UniversalRedisQueue;
import redis.clients.jedis.Jedis;

public class MyQueue extends UniversalRedisQueue {
    public void flush() {
        try (Jedis jedis = getJedisConnector().getJedis()) {
            jedis.flushAll();
        }
    }
}

package com.github.ddth.queue.test.universal.idint.redis;

import com.github.ddth.queue.impl.universal.idint.UniversalRedisQueue;
import redis.clients.jedis.Jedis;

public class MyQueue extends UniversalRedisQueue {
    public void flush() {
        try (Jedis jedis = getJedisConnector().getJedis()) {
            jedis.flushAll();
        }
    }
}

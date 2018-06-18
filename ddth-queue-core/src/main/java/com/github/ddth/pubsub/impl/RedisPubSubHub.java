package com.github.ddth.pubsub.impl;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ExecutionException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.commons.redis.JedisUtils;
import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.ISubscriber;
import com.github.ddth.queue.IMessage;
import com.github.ddth.queue.utils.QueueUtils;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;

import redis.clients.jedis.BinaryJedisPubSub;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Protocol;

/**
 * Redis implementation of {@link IPubSubHub}.
 * 
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class RedisPubSubHub<ID, DATA> extends BaseRedisPubSubHub<ID, DATA> {

    private final Logger LOGGER = LoggerFactory.getLogger(RedisPubSubHub.class);

    public final static String DEFAULT_HOST_AND_PORT = Protocol.DEFAULT_HOST + ":"
            + Protocol.DEFAULT_PORT;

    private String redisHostAndPort = DEFAULT_HOST_AND_PORT;

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
    public RedisPubSubHub<ID, DATA> setRedisHostAndPort(String redisHostAndPort) {
        this.redisHostAndPort = redisHostAndPort;
        return this;
    }

    /*----------------------------------------------------------------------*/
    private boolean ready = false;

    /**
     * Return {@code true} if this hub is ready for subscribing.
     * 
     * @return
     */
    public boolean isReady() {
        return ready;
    }

    private LoadingCache<String, Set<ISubscriber<ID, DATA>>> subscriptions = CacheBuilder
            .newBuilder().build(new CacheLoader<String, Set<ISubscriber<ID, DATA>>>() {
                @Override
                public Set<ISubscriber<ID, DATA>> load(String key) throws Exception {
                    return new HashSet<>();
                }
            });

    private BinaryJedisPubSub myPubSubGateway = new BinaryJedisPubSub() {
        /**
         * {@inheritDoc}
         */
        @Override
        public void onSubscribe(byte[] channel, int subscribedChannels) {
            ready = true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onUnsubscribe(byte[] channel, int subscribedChannels) {
            ready = false;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onPSubscribe(byte[] channel, int subscribedChannels) {
            ready = true;
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onPUnsubscribe(byte[] channel, int subscribedChannels) {
            ready = false;
        }

        private void handleMessage(byte[] _channel, byte[] _message) {
            String channel = new String(_channel, QueueUtils.UTF8);
            try {
                Set<ISubscriber<ID, DATA>> subs = subscriptions.get(channel);
                if (subs != null && subs.size() > 0) {
                    IMessage<ID, DATA> message = deserialize(_message);
                    synchronized (subs) {
                        for (ISubscriber<ID, DATA> sub : subs) {
                            try {
                                sub.onMessage(channel, message);
                            } catch (Exception e) {
                                LOGGER.warn(e.getMessage(), e);
                            }
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error(e.getMessage(), e);
            }
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onPMessage(byte[] pattern, byte[] channel, byte[] message) {
            handleMessage(channel, message);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public void onMessage(byte[] channel, byte[] message) {
            handleMessage(channel, message);
        }
    };
    private Jedis myPubSubJedis;

    /**
     * {@inheritDoc}
     */
    @Override
    protected JedisConnector buildJedisConnector() {
        JedisConnector jedisConnector = new JedisConnector();
        jedisConnector.setJedisPoolConfig(JedisUtils.defaultJedisPoolConfig())
                .setRedisHostsAndPorts(getRedisHostAndPort()).setRedisPassword(getRedisPassword())
                .init();
        return jedisConnector;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RedisPubSubHub<ID, DATA> init() {
        super.init();

        new Thread() {
            public void run() {
                myPubSubJedis = getJedisConnector().getJedis();
                myPubSubJedis.psubscribe(myPubSubGateway, "*".getBytes());
            }
        }.start();

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        try {
            try {
                if (myPubSubGateway != null) {
                    myPubSubGateway.punsubscribe();
                    long now = System.currentTimeMillis();
                    while (myPubSubGateway.isSubscribed()
                            && System.currentTimeMillis() - now < 1000) {
                        Thread.sleep(1);
                    }
                }
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            }

            try {
                if (myPubSubJedis != null) {
                    myPubSubJedis.close();
                }
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            }
        } finally {
            super.destroy();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean publish(String channel, IMessage<ID, DATA> msg) {
        try (Jedis jedis = getJedisConnector().getJedis()) {
            byte[] message = serialize(msg);
            Long result = jedis.publish(channel.getBytes(QueueUtils.UTF8), message);
            return result != null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(String channel, ISubscriber<ID, DATA> subscriber) {
        try {
            Set<ISubscriber<ID, DATA>> subs = subscriptions.get(channel);
            synchronized (subs) {
                subs.add(subscriber);
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void unsubscribe(String channel, ISubscriber<ID, DATA> subscriber) {
        try {
            Set<ISubscriber<ID, DATA>> subs = subscriptions.get(channel);
            synchronized (subs) {
                subs.remove(subscriber);
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}

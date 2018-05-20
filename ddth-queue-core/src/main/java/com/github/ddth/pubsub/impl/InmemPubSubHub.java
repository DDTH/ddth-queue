package com.github.ddth.pubsub.impl;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;

import com.github.ddth.pubsub.IPubSubHub;
import com.github.ddth.pubsub.ISubscriber;
import com.github.ddth.queue.IMessage;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.eventbus.EventBus;
import com.google.common.eventbus.Subscribe;

/**
 * In-Memory implementation of {@link IPubSubHub}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class InmemPubSubHub<ID, DATA> extends AbstractPubSubHub<ID, DATA> {

    private class WrapAroundSubscriber {
        private ISubscriber<ID, DATA> subscriber;
        private String channel;

        public WrapAroundSubscriber(String channel, ISubscriber<ID, DATA> subscriber) {
            this.channel = channel;
            this.subscriber = subscriber;
        }

        @Subscribe
        public void onEvent(IMessage<ID, DATA> msg) {
            subscriber.onMessage(channel, msg);
        }
    }

    private LoadingCache<String, EventBus> eventBus = CacheBuilder.newBuilder()
            .build(new CacheLoader<String, EventBus>() {
                @Override
                public EventBus load(String key) throws Exception {
                    return new EventBus();
                }
            });

    private LoadingCache<String, Cache<ISubscriber<ID, DATA>, WrapAroundSubscriber>> subscriptions = CacheBuilder
            .newBuilder()
            .build(new CacheLoader<String, Cache<ISubscriber<ID, DATA>, WrapAroundSubscriber>>() {
                @Override
                public Cache<ISubscriber<ID, DATA>, WrapAroundSubscriber> load(String key)
                        throws Exception {
                    return CacheBuilder.newBuilder().build();
                }
            });

    /**
     * {@inheritDoc}
     */
    @Override
    public InmemPubSubHub<ID, DATA> init() {
        super.init();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        try {
            eventBus.invalidateAll();
            subscriptions.invalidateAll();
        } finally {
            super.destroy();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean publish(String channel, IMessage<ID, DATA> msg) {
        try {
            eventBus.get(channel).post(msg);
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void subscribe(String channel, ISubscriber<ID, DATA> subscriber) {
        try {
            WrapAroundSubscriber wrap = subscriptions.get(channel).get(subscriber,
                    new Callable<WrapAroundSubscriber>() {
                        @Override
                        public WrapAroundSubscriber call() throws Exception {
                            return new WrapAroundSubscriber(channel, subscriber);
                        }
                    });
            eventBus.get(channel).register(wrap);
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
            WrapAroundSubscriber wrap = subscriptions.get(channel).getIfPresent(subscriber);
            if (wrap != null) {
                eventBus.get(channel).unregister(wrap);
            }
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

}

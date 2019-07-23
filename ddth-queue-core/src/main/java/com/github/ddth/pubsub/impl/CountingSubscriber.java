package com.github.ddth.pubsub.impl;

import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import com.github.ddth.pubsub.ISubscriber;
import com.github.ddth.queue.IMessage;
import com.google.common.util.concurrent.AtomicLongMap;

/**
 * Count number of received messages.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class CountingSubscriber<ID, DATA> implements ISubscriber<ID, DATA> {
    private AtomicLong counterAll = new AtomicLong(0);
    private AtomicLongMap<String> counterMap = AtomicLongMap.create();

    /**
     * Reset counter to value zero.
     */
    public void resetCounter() {
        counterAll.set(0);
        counterMap.clear();
    }

    /**
     * Get total number of received messages.
     * 
     * @return
     */
    public long getTotalMessages() {
        return counterAll.get();
    }

    /**
     * Get all counters as {@code channel:value}.
     * 
     * @return
     */
    public Map<String, Long> getAllCounters() {
        return counterMap.asMap();
    }

    /**
     * Get total number of received messages on a channel.
     * 
     * @param channel
     * @return
     */
    public long getCounter(String channel) {
        return counterMap.get(channel);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean onMessage(String channel, IMessage<ID, DATA> msg) {
        counterAll.incrementAndGet();
        counterMap.incrementAndGet(channel);
        return true;
    }
}

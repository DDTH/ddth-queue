package com.github.ddth.queue.impl;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;

import java.util.Collection;
import java.util.HashSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * Abstract queue implementation that uses in-memory ephemeral storage.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.0.0
 */
public abstract class AbstractInmemEphemeralQueue<ID, DATA> extends AbstractEphemeralSupportQueue<ID, DATA> {
    private ConcurrentMap<Object, IQueueMessage<ID, DATA>> ephemeralStorage;

    public AbstractInmemEphemeralQueue<ID, DATA> setEphemeralDisabled(boolean ephemeralDisabled) {
        super.setEphemeralDisabled(ephemeralDisabled);
        if (isEphemeralDisabled()) {
            ephemeralStorage = null;
        }
        return this;
    }

    protected void initEphemeralStorage(int hintSize) {
        if (!isEphemeralDisabled()) {
            int ephemeralBoundSize = Math.max(0, getEphemeralMaxSize());
            ephemeralStorage = new ConcurrentHashMap<>(
                    ephemeralBoundSize > 0 ? Math.min(ephemeralBoundSize, hintSize) : hintSize);
        } else {
            ephemeralStorage = null;
        }
    }

    protected void doRemoveFromEphemeralStorage(IQueueMessage<ID, DATA> msg) {
        if (ephemeralStorage != null) {
            ephemeralStorage.remove(msg.getId());
        }
    }

    protected void ensureEphemeralSize() {
        if (ephemeralStorage != null) {
            int ephemeralMaxSize = getEphemeralMaxSize();
            if (ephemeralMaxSize > 0 && ephemeralStorage.size() >= ephemeralMaxSize) {
                throw new QueueException.EphemeralIsFull(ephemeralMaxSize);
            }
        }
    }

    protected void doPutToEphemeralStorage(IQueueMessage<ID, DATA> msg) {
        if (msg != null && ephemeralStorage != null) {
            ephemeralStorage.putIfAbsent(msg.getId(), msg);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<IQueueMessage<ID, DATA>> getOrphanMessages(long thresholdTimestampMs) {
        Collection<IQueueMessage<ID, DATA>> orphanMessages = new HashSet<>();
        if (ephemeralStorage != null) {
            long now = System.currentTimeMillis();
            ephemeralStorage.forEach((key, msg) -> {
                if (msg.getQueueTimestamp().getTime() + thresholdTimestampMs < now)
                    orphanMessages.add(msg);
            });
        }
        return orphanMessages;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        return ephemeralStorage != null ? ephemeralStorage.size() : 0;
    }
}

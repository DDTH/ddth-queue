package com.github.ddth.queue.impl;

import com.github.ddth.queue.QueueSpec;

/**
 * Factory to create {@link InmemQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public abstract class InmemQueueFactory<T extends InmemQueue<ID, DATA>, ID, DATA>
        extends AbstractQueueFactory<T, ID, DATA> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected void initQueue(T queue, QueueSpec spec) {
        super.initQueue(queue, spec);

        queue.setBoundary(getDefaultMaxSize()).setEphemeralDisabled(getDefaultEphemeralDisabled())
                .setEphemeralMaxSize(getDefaultEphemeralMaxSize());

        Integer maxSize = spec.getField(QueueSpec.FIELD_MAX_SIZE, Integer.class);
        if (maxSize != null) {
            queue.setBoundary(maxSize.intValue());
        }

        Boolean ephemeralDisabled = spec.getField(QueueSpec.FIELD_EPHEMERAL_DISABLED,
                Boolean.class);
        if (ephemeralDisabled != null) {
            queue.setEphemeralDisabled(ephemeralDisabled.booleanValue());
        }

        Integer maxEphemeralSize = spec.getField(QueueSpec.FIELD_EPHEMERAL_MAX_SIZE, Integer.class);
        if (maxEphemeralSize != null) {
            queue.setEphemeralMaxSize(maxEphemeralSize.intValue());
        }

        queue.init();
    }

}

package com.github.ddth.queue.impl;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.queue.QueueSpec;

/**
 * Factory to create {@link RabbitMqQueue} instances.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.1
 */
public abstract class RabbitMqQueueFactory<T extends RabbitMqQueue<ID, DATA>, ID, DATA>
        extends AbstractQueueFactory<T, ID, DATA> {

    public final static String SPEC_FIELD_URI = "uri";
    public final static String SPEC_FIELD_QUEUE_NAME = "queue_name";

    private String defaultUri = RabbitMqQueue.DEFAULT_URI,
            defaultQueueName = RabbitMqQueue.DEFAULT_QUEUE_NAME;

    public String getDefaultUri() {
        return defaultUri;
    }

    public void setDefaultUri(String defaultUri) {
        this.defaultUri = defaultUri;
    }

    public String getDefaultQueueName() {
        return defaultQueueName;
    }

    public void setDefaultQueueName(String defaultQueueName) {
        this.defaultQueueName = defaultQueueName;
    }

    /**
     * {@inheritDoc}
     * @throws Exception 
     */
    @Override
    protected void initQueue(T queue, QueueSpec spec) throws Exception {
        super.initQueue(queue, spec);

        queue.setUri(defaultUri).setQueueName(defaultQueueName);

        String uri = spec.getField(SPEC_FIELD_URI);
        if (!StringUtils.isBlank(uri)) {
            queue.setUri(uri);
        }

        String queueName = spec.getField(SPEC_FIELD_QUEUE_NAME);
        if (!StringUtils.isBlank(uri)) {
            queue.setQueueName(queueName);
        }

        queue.init();
    }

}

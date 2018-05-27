package com.github.ddth.queue.impl;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.queue.QueueSpec;
import com.rabbitmq.client.ConnectionFactory;

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

    private ConnectionFactory defaultConnectionFactory;
    private boolean myOwnConnectionFactory;
    private String defaultUri = RabbitMqQueue.DEFAULT_URI,
            defaultQueueName = RabbitMqQueue.DEFAULT_QUEUE_NAME;

    public String getDefaultUri() {
        return defaultUri;
    }

    public RabbitMqQueueFactory<T, ID, DATA> setDefaultUri(String defaultUri) {
        this.defaultUri = defaultUri;
        return this;
    }

    public String getDefaultQueueName() {
        return defaultQueueName;
    }

    public RabbitMqQueueFactory<T, ID, DATA> setDefaultQueueName(String defaultQueueName) {
        this.defaultQueueName = defaultQueueName;
        return this;
    }

    /**
     * Getter for {@link #defaultConnectionFactory}.
     * 
     * @return
     * @since 0.7.1
     */
    protected ConnectionFactory getDefaultConnectionFactory() {
        return defaultConnectionFactory;
    }

    /**
     * Setter for {@link #defaultConnectionFactory}.
     * 
     * @param connectionFactory
     * @param setMyOwnConnectionFactory
     * @return
     * @since 0.7.1
     */
    protected RabbitMqQueueFactory<T, ID, DATA> setDefaultConnectionFactory(
            ConnectionFactory connectionFactory, boolean setMyOwnConnectionFactory) {
        if (myOwnConnectionFactory && this.defaultConnectionFactory != null) {
            // destroy this.defaultConnectionFactory
        }
        this.defaultConnectionFactory = connectionFactory;
        myOwnConnectionFactory = setMyOwnConnectionFactory;
        return this;
    }

    /**
     * Setter for {@link #defaultConnectionFactory}.
     * 
     * @param connectionFactory
     * @return
     * @since 0.7.1
     */
    public RabbitMqQueueFactory<T, ID, DATA> setDefaultConnectionFactory(
            ConnectionFactory connectionFactory) {
        return setDefaultConnectionFactory(connectionFactory, false);
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        try {
            super.destroy();
        } finally {
            if (defaultConnectionFactory != null && myOwnConnectionFactory) {
                defaultConnectionFactory = null;
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @throws Exception
     */
    @Override
    protected void initQueue(T queue, QueueSpec spec) throws Exception {
        queue.setConnectionFactory(defaultConnectionFactory).setUri(defaultUri)
                .setQueueName(defaultQueueName);

        String uri = spec.getField(SPEC_FIELD_URI);
        if (!StringUtils.isBlank(uri)) {
            queue.setUri(uri);
        }

        String queueName = spec.getField(SPEC_FIELD_QUEUE_NAME);
        if (!StringUtils.isBlank(uri)) {
            queue.setQueueName(queueName);
        }

        super.initQueue(queue, spec);
    }

}

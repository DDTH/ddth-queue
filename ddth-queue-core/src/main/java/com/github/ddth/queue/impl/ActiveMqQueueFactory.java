package com.github.ddth.queue.impl;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.StringUtils;

import com.github.ddth.queue.QueueSpec;

/**
 * Factory to create {@link ActiveMqQueue} instances.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.1
 */
public abstract class ActiveMqQueueFactory<T extends ActiveMqQueue<ID, DATA>, ID, DATA>
        extends AbstractQueueFactory<T, ID, DATA> {

    public final static String SPEC_FIELD_URI = "uri";
    public final static String SPEC_FIELD_USERNAME = "username";
    public final static String SPEC_FIELD_PASSWORD = "password";
    public final static String SPEC_FIELD_QUEUE_NAME = "queue_name";

    private String defaultUri = ActiveMqQueue.DEFAULT_URI,
            defaultQueueName = ActiveMqQueue.DEFAULT_QUEUE_NAME;
    private String defaultUsername = null, defaultPassword = null;

    public String getDefaultUri() {
        return defaultUri;
    }

    public ActiveMqQueueFactory<T, ID, DATA> setDefaultUri(String defaultUri) {
        this.defaultUri = defaultUri;
        return this;
    }

    public String getDefaultQueueName() {
        return defaultQueueName;
    }

    public ActiveMqQueueFactory<T, ID, DATA> setDefaultQueueName(String defaultQueueName) {
        this.defaultQueueName = defaultQueueName;
        return this;
    }

    public String getDefaultUsername() {
        return defaultUsername;
    }

    public ActiveMqQueueFactory<T, ID, DATA> setDefaultUsername(String defaultUsername) {
        this.defaultUsername = defaultUsername;
        return this;
    }

    public String getDefaultPassword() {
        return defaultPassword;
    }

    public ActiveMqQueueFactory<T, ID, DATA> setDefaultPassword(String defaultPassword) {
        this.defaultPassword = defaultPassword;
        return this;
    }

    private ActiveMQConnectionFactory defaultConnectionFactory;
    private boolean myOwnConnectionFactory = false;

    /**
     * Getter for {@link #defaultConnectionFactory}.
     * 
     * <p>
     * If all {@link ActiveMqQueue} instances are connecting to one ActiveMQ
     * broker, it's a good idea to pre-create an
     * {@link ActiveMQConnectionFactory} instance and share it amongst
     * {@link ActiveMqQueue} instances created from this factory by assigning it
     * to {@link #defaultConnectionFactory} (see
     * {@link #setDefaultConnectionFactory(ActiveMQConnectionFactory)}).
     * </p>
     * 
     * @return
     * @since 0.7.1
     */
    protected ActiveMQConnectionFactory getDefaultConnectionFactory() {
        return defaultConnectionFactory;
    }

    /**
     * Setter for {@link #defaultConnectionFactory}.
     * 
     * @param connectionFactory
     * @return
     * @since 0.7.1
     */
    public ActiveMqQueueFactory<T, ID, DATA> setDefaultConnectionFactory(
            ActiveMQConnectionFactory connectionFactory) {
        return setDefaultConnectionFactory(connectionFactory, false);
    }

    /**
     * Setter for {@link #defaultConnectionFactory}.
     * 
     * @param connectionFactory
     * @param setMyOwnConnectionFactory
     * @return
     * @since 0.7.1
     */
    protected ActiveMqQueueFactory<T, ID, DATA> setDefaultConnectionFactory(
            ActiveMQConnectionFactory connectionFactory, boolean setMyOwnConnectionFactory) {
        if (myOwnConnectionFactory && this.defaultConnectionFactory != null) {
            // destroy this.defaultConnectionFactory instance
        }
        this.defaultConnectionFactory = connectionFactory;
        myOwnConnectionFactory = setMyOwnConnectionFactory;
        return this;
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.7.1
     */
    @Override
    public void destroy() {
        try {
            super.destroy();
        } finally {
            if (myOwnConnectionFactory && defaultConnectionFactory != null) {
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
        queue.setUri(defaultUri).setQueueName(defaultQueueName).setUsername(defaultUsername)
                .setPassword(defaultPassword).setConnectionFactory(defaultConnectionFactory);

        String uri = spec.getField(SPEC_FIELD_URI);
        if (!StringUtils.isBlank(uri)) {
            queue.setUri(uri);
        }

        String queueName = spec.getField(SPEC_FIELD_QUEUE_NAME);
        if (!StringUtils.isBlank(uri)) {
            queue.setQueueName(queueName);
        }

        String username = spec.getField(SPEC_FIELD_USERNAME);
        if (!StringUtils.isBlank(username)) {
            queue.setUsername(username);
        }

        String password = spec.getField(SPEC_FIELD_PASSWORD);
        if (!StringUtils.isBlank(password)) {
            queue.setPassword(password);
        }

        super.initQueue(queue, spec);
    }

}

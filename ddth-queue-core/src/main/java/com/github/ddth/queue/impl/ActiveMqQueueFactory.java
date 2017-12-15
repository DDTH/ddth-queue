package com.github.ddth.queue.impl;

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

    public void setDefaultUri(String defaultUri) {
        this.defaultUri = defaultUri;
    }

    public String getDefaultQueueName() {
        return defaultQueueName;
    }

    public void setDefaultQueueName(String defaultQueueName) {
        this.defaultQueueName = defaultQueueName;
    }

    public String getDefaultUsername() {
        return defaultUsername;
    }

    public void setDefaultUsername(String defaultUsername) {
        this.defaultUsername = defaultUsername;
    }

    public String getDefaultPassword() {
        return defaultPassword;
    }

    public void setDefaultPassword(String defaultPassword) {
        this.defaultPassword = defaultPassword;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void initQueue(T queue, QueueSpec spec) {
        super.initQueue(queue, spec);

        queue.setUri(defaultUri).setQueueName(defaultQueueName).setUsername(defaultUsername)
                .setPassword(defaultPassword);

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

        queue.init();
    }

}

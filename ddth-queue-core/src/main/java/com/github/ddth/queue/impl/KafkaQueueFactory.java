package com.github.ddth.queue.impl;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaClient.ProducerType;
import com.github.ddth.queue.QueueSpec;

/**
 * Factory to create {@link KafkaQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public abstract class KafkaQueueFactory<T extends KafkaQueue<ID, DATA>, ID, DATA>
        extends AbstractQueueFactory<T, ID, DATA> {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaQueueFactory.class);

    public final static String SPEC_FIELD_BOOTSTRAP_SERVERS = "bootstrap_servers";
    public final static String SPEC_FIELD_TOPIC = "topic";
    public final static String SPEC_FIELD_CONSUMER_GROUP_ID = "consumer_group_id";
    public final static String SPEC_FIELD_PRODUCER_TYPE = "producer_type";
    public final static String SPEC_FIELD_PRODUCER_PROPERTIES = "producer_properties";
    public final static String SPEC_FIELD_CONSUMER_PROPERTIES = "consumer_properties";
    public final static String SPEC_FIELD_SEND_ASYNC = "send_async";

    private KafkaClient defaultKafkaClient;
    private boolean myOwnKafkaClient;
    private String defaultBootstrapServers = KafkaQueue.DEFAULT_BOOTSTRAP_SERVERS;
    private String defaultTopicName = KafkaQueue.DEFAULT_TOPIC_NAME;
    private String defaultConsumerGroupId;
    private ProducerType defaultProducerType = KafkaQueue.DEFAULT_PRODUCER_TYPE;
    private Properties defaultProducerProps, defaultConsumerProps;
    private boolean defaultSendAsync = KafkaQueue.DEFAULT_SEND_ASYNC;

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultBootstrapServers() {
        return defaultBootstrapServers;
    }

    /**
     * 
     * @param defaultBootstrapServers
     * @since 0.6.2
     */
    public void setDefaultBootstrapServers(String defaultBootstrapServers) {
        this.defaultBootstrapServers = defaultBootstrapServers;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultTopicName() {
        return defaultTopicName;
    }

    /**
     * 
     * @param defaultTopicName
     * @since 0.6.2
     */
    public void setDefaultTopicName(String defaultTopicName) {
        this.defaultTopicName = defaultTopicName;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultConsumerGroupId() {
        return defaultConsumerGroupId;
    }

    /**
     * 
     * @param defaultConsumerGroupId
     * @since 0.6.2
     */
    public void setDefaultConsumerGroupId(String defaultConsumerGroupId) {
        this.defaultConsumerGroupId = defaultConsumerGroupId;
    }

    public ProducerType getDefaultProducerType() {
        return defaultProducerType;
    }

    public void setDefaultProducerType(ProducerType defaultProducerType) {
        this.defaultProducerType = defaultProducerType;
    }

    public Properties getDefaultProducerProps() {
        return defaultProducerProps;
    }

    public void setDefaultProducerProps(Properties defaultProducerProps) {
        this.defaultProducerProps = defaultProducerProps;
    }

    public Properties getDefaultConsumerProps() {
        return defaultConsumerProps;
    }

    public void setDefaultConsumerProps(Properties defaultConsumerProps) {
        this.defaultConsumerProps = defaultConsumerProps;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public boolean isDefaultSendAsync() {
        return defaultSendAsync;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public boolean getDefaultSendAsync() {
        return defaultSendAsync;
    }

    /**
     * 
     * @param defaultSendAsync
     * @since 0.6.2
     */
    public void setDefaultSendAsync(boolean defaultSendAsync) {
        this.defaultSendAsync = defaultSendAsync;
    }

    /**
     * Getter for {@link #defaultKafkaClient}.
     * 
     * <p>
     * If all {@link KafkaQueue} instances are connecting to one Kafka broker,
     * it's a good idea to pre-create a {@link KafkaClient} instance and share
     * it amongst {@link KafkaQueue} instances created from this factory by
     * assigning it to {@link #defaultKafkaClient} (see
     * {@link #setDefaultKafkaClient(KafkaClient)}).
     * </p>
     * 
     * @return
     * @since 0.7.1
     */
    protected KafkaClient getDefaultKafkaClient() {
        return defaultKafkaClient;
    }

    /**
     * Setter for {@link #defaultKafkaClient}.
     * 
     * @param kafkaClient
     * @param setMyOwnKafkaClient
     * @return
     * @since 0.7.1
     */
    protected KafkaQueueFactory<T, ID, DATA> setDefaultKafkaClient(KafkaClient kafkaClient,
            boolean setMyOwnKafkaClient) {
        if (this.defaultKafkaClient != null && myOwnKafkaClient) {
            this.defaultKafkaClient.destroy();
        }
        this.defaultKafkaClient = kafkaClient;
        myOwnKafkaClient = setMyOwnKafkaClient;
        return this;
    }

    /**
     * Setter for {@link #defaultKafkaClient}.
     * 
     * @param kafkaClient
     * @return
     * @since 0.7.1
     */
    public KafkaQueueFactory<T, ID, DATA> setDefaultKafkaClient(KafkaClient kafkaClient) {
        return setDefaultKafkaClient(kafkaClient, false);
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
            if (myOwnKafkaClient && defaultKafkaClient != null) {
                try {
                    defaultKafkaClient.destroy();
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                } finally {
                    defaultKafkaClient = null;
                }
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
        queue.setKafkaClient(defaultKafkaClient).setProducerType(defaultProducerType)
                .setKafkaProducerProperties(defaultProducerProps)
                .setKafkaConsumerProperties(defaultConsumerProps);
        queue.setSendAsync(defaultSendAsync);

        String bootstrapServers = spec.getField(SPEC_FIELD_BOOTSTRAP_SERVERS);
        bootstrapServers = StringUtils.isBlank(bootstrapServers) ? defaultBootstrapServers
                : bootstrapServers;
        if (StringUtils.isBlank(bootstrapServers)) {
            throw new IllegalArgumentException(
                    "Empty or Invalid value for param [" + SPEC_FIELD_BOOTSTRAP_SERVERS + "]!");
        }
        queue.setKafkaBootstrapServers(bootstrapServers);

        String consumerGroupId = spec.getField(SPEC_FIELD_CONSUMER_GROUP_ID);
        consumerGroupId = StringUtils.isBlank(consumerGroupId) ? defaultConsumerGroupId
                : consumerGroupId;
        if (!StringUtils.isBlank(consumerGroupId)) {
            queue.setConsumerGroupId(consumerGroupId);
        }

        String topicName = spec.getField(SPEC_FIELD_TOPIC);
        topicName = StringUtils.isBlank(topicName) ? defaultTopicName : topicName;
        if (StringUtils.isBlank(topicName)) {
            throw new IllegalArgumentException(
                    "Empty or Invalid value for param [" + SPEC_FIELD_TOPIC + "]!");
        }
        queue.setTopicName(topicName);

        String producerTypeStr = spec.getField(SPEC_FIELD_PRODUCER_TYPE);
        if (!StringUtils.isBlank(producerTypeStr)) {
            try {
                ProducerType producerType = ProducerType.valueOf(producerTypeStr);
                if (producerType != null) {
                    queue.setProducerType(producerType);
                }
            } catch (Exception e) {
            }
        }

        Properties producerProps = spec.getField(SPEC_FIELD_PRODUCER_PROPERTIES, Properties.class);
        if (producerProps != null) {
            queue.setKafkaProducerProperties(producerProps);
        }

        Properties consumerProps = spec.getField(SPEC_FIELD_CONSUMER_PROPERTIES, Properties.class);
        if (consumerProps != null) {
            queue.setKafkaConsumerProperties(consumerProps);
        }

        Boolean sendAsync = spec.getField(SPEC_FIELD_SEND_ASYNC, Boolean.class);
        if (sendAsync != null) {
            queue.setSendAsync(sendAsync.booleanValue());
        }

        super.initQueue(queue, spec);
    }

}

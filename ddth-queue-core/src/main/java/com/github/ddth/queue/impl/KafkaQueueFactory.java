package com.github.ddth.queue.impl;

import java.util.Properties;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.kafka.KafkaClient.ProducerType;
import com.github.ddth.queue.QueueSpec;

/**
 * Factory to create {@link KafkaQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public abstract class KafkaQueueFactory<T extends KafkaQueue> extends AbstractQueueFactory<T> {

    public final static String SPEC_FIELD_CONSUMER_GROUP_ID = "consumer_group_id";
    public final static String SPEC_FIELD_BOOTSTRAP_SERVERS = "bootstrap_servers";
    public final static String SPEC_FIELD_TOPIC = "topic";
    public final static String SPEC_FIELD_PRODUCER_TYPE = "producer_type";
    public final static String SPEC_FIELD_PRODUCER_PROPERTIES = "producer_properties";
    public final static String SPEC_FIELD_CONSUMER_PROPERTIES = "consumer_properties";

    private ProducerType defaultProducerType = ProducerType.SYNC_LEADER_ACK;
    private Properties defaultProducerProps, defaultConsumerProps;

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
     * {@inheritDoc}
     */
    @Override
    protected void initQueue(T queue, QueueSpec spec) {
        queue.setProducerType(defaultProducerType).setKafkaProducerProperties(defaultProducerProps)
                .setKafkaConsumerProperties(defaultConsumerProps);

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

        String consumerGroupId = spec.getField(SPEC_FIELD_CONSUMER_GROUP_ID);
        if (!StringUtils.isBlank(consumerGroupId)) {
            queue.setConsumerGroupId(consumerGroupId);
        }

        String bootstrapServers = spec.getField(SPEC_FIELD_BOOTSTRAP_SERVERS);
        if (StringUtils.isBlank(bootstrapServers)) {
            throw new IllegalArgumentException(
                    "Empty or Invalid value for param [" + SPEC_FIELD_BOOTSTRAP_SERVERS + "]!");
        }
        queue.setKafkaBootstrapServers(bootstrapServers);

        String topicName = spec.getField(SPEC_FIELD_TOPIC);
        if (StringUtils.isBlank(topicName)) {
            throw new IllegalArgumentException(
                    "Empty or Invalid value for param [" + SPEC_FIELD_TOPIC + "]!");
        }
        queue.setTopicName(topicName);

        try {
            queue.init();
        } catch (Exception e) {
            throw e instanceof RuntimeException ? (RuntimeException) e : new RuntimeException(e);
        }
    }

}

package com.github.ddth.queue.impl;

import java.util.Collection;
import java.util.Date;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaClient.ProducerType;
import com.github.ddth.kafka.KafkaMessage;
import com.github.ddth.queue.IPartitionSupport;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;

/**
 * (Experimental) Kafka implementation of {@link IQueue}.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.2
 */
public abstract class KafkaQueue<ID, DATA> extends AbstractQueue<ID, DATA> {

    private final Logger LOGGER = LoggerFactory.getLogger(KafkaQueue.class);

    public final static String DEFAULT_BOOTSTRAP_SERVERS = "localhost:9092";
    public final static String DEFAULT_TOPIC_NAME = "ddth-queue";
    public final static ProducerType DEFAULT_PRODUCER_TYPE = ProducerType.LEADER_ACK;
    public final static boolean DEFAULT_SEND_ASYNC = true;
    public final static String DEFAULT_CONSUMER_GROUP_ID = "ddth-queue";

    private KafkaClient kafkaClient;
    private boolean myOwnKafkaClient = true;

    private String bootstrapServers = DEFAULT_BOOTSTRAP_SERVERS;
    private String topicName = DEFAULT_TOPIC_NAME;
    private String consumerGroupId = DEFAULT_CONSUMER_GROUP_ID + System.currentTimeMillis();
    private ProducerType producerType = DEFAULT_PRODUCER_TYPE;
    private Properties producerProps, consumerProps;
    private boolean sendAsync = DEFAULT_SEND_ASYNC;

    /**
     * Sends message to Kafka asynchronously or not (default {@code true}).
     *
     * @return
     * @since 0.5.0
     */
    public boolean isSendAsync() {
        return sendAsync;
    }

    /**
     * Sends message to Kafka asynchronously or not.
     *
     * @param value
     * @return
     */
    public KafkaQueue<ID, DATA> setSendAsync(boolean value) {
        this.sendAsync = value;
        return this;
    }

    public ProducerType getProducerType() {
        return producerType;
    }

    public KafkaQueue<ID, DATA> setProducerType(ProducerType producerType) {
        this.producerType = producerType;
        return this;
    }

    /**
     * Kafka bootstrap server list (format
     * {@code host1:9092,host2:port2,host3:port3}).
     *
     * @return
     * @since 0.4.0
     */
    public String getKafkaBootstrapServers() {
        return bootstrapServers;
    }

    /**
     * Sets Kafka bootstrap server list (format
     * {@code host1:9092,host2:port2,host3:port3}).
     *
     * @param kafkaBootstrapServers
     * @return
     * @since 0.4.0
     */
    public KafkaQueue<ID, DATA> setKafkaBootstrapServers(String kafkaBootstrapServers) {
        this.bootstrapServers = kafkaBootstrapServers;
        return this;
    }

    /**
     * Gets Kafka producer's custom configuration properties.
     *
     * @return
     * @since 0.4.0
     */
    public Properties getKafkaProducerProperties() {
        return producerProps;
    }

    /**
     * Sets Kafka producer's custom configuration properties.
     *
     * @param kafkaProducerConfigs
     * @return
     * @since 0.4.0
     */
    public KafkaQueue<ID, DATA> setKafkaProducerProperties(Properties kafkaProducerConfigs) {
        this.producerProps = kafkaProducerConfigs;
        return this;
    }

    /**
     * Gets Kafka consumer's custom configuration properties.
     *
     * @return
     * @since 0.4.0
     */
    public Properties getKafkaConsumerProperties() {
        return consumerProps;
    }

    /**
     * Sets Kafka consumer's custom configuration properties.
     *
     * @param kafkaConsumerConfigs
     * @return
     * @since 0.4.0
     */
    public KafkaQueue<ID, DATA> setKafkaConsumerProperties(Properties kafkaConsumerConfigs) {
        this.consumerProps = kafkaConsumerConfigs;
        return this;
    }

    /**
     * Name of Kafka topic to store queue messages.
     *
     * @return
     */
    public String getTopicName() {
        return topicName;
    }

    public KafkaQueue<ID, DATA> setTopicName(String topicName) {
        this.topicName = topicName;
        return this;
    }

    /**
     * Kafka's group-id to consume messages.
     *
     * @return
     */
    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public KafkaQueue<ID, DATA> setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
        return this;
    }

    protected KafkaClient getKafkaClient() {
        return kafkaClient;
    }

    /**
     * Setter for {@link #kafkaClient}.
     * 
     * @param kafkaClient
     * @param setMyOwnKafkaClient
     * @return
     */
    protected KafkaQueue<ID, DATA> setKafkaClient(KafkaClient kafkaClient,
            boolean setMyOwnKafkaClient) {
        if (this.kafkaClient != null && myOwnKafkaClient) {
            this.kafkaClient.destroy();
        }
        this.kafkaClient = kafkaClient;
        myOwnKafkaClient = setMyOwnKafkaClient;
        return this;
    }

    /**
     * An external {@link KafkaClient} can be used. If not set,
     * {@link KafkaQueue} will automatically create a {@link KafkaClient} for
     * its own use.
     *
     * @param kafkaClient
     * @return
     */
    public KafkaQueue<ID, DATA> setKafkaClient(KafkaClient kafkaClient) {
        return setKafkaClient(kafkaClient, false);
    }

    /*----------------------------------------------------------------------*/
    protected KafkaClient buildKafkaClient() throws Exception {
        if (StringUtils.isBlank(bootstrapServers)) {
            throw new IllegalStateException("Kafka bootstrap server list is not defined.");
        }
        KafkaClient kafkaClient = new KafkaClient(bootstrapServers);
        kafkaClient.setProducerProperties(consumerProps).setConsumerProperties(consumerProps);
        kafkaClient.init();
        return kafkaClient;
    }

    /**
     * Init method.
     *
     * @return
     * @throws Exception
     */
    public KafkaQueue<ID, DATA> init() throws Exception {
        if (kafkaClient == null) {
            setKafkaClient(buildKafkaClient(), true);
        }

        super.init();

        if (kafkaClient == null) {
            throw new IllegalStateException("Kafka client is null.");
        }

        return this;
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        try {
            super.destroy();
        } finally {
            if (kafkaClient != null && myOwnKafkaClient) {
                try {
                    kafkaClient.destroy();
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                } finally {
                    kafkaClient = null;
                }
            }
        }
    }

    /**
     * Takes a message from Kafka queue.
     *
     * @return
     * @since 0.3.3
     */
    protected IQueueMessage<ID, DATA> takeFromQueue() {
        KafkaMessage kMsg = kafkaClient.consumeMessage(consumerGroupId, true, topicName, 1000,
                TimeUnit.MILLISECONDS);
        return kMsg != null ? deserialize(kMsg.content()) : null;
    }

    /**
     * Puts a message to Kafka queue, partitioning message by
     * {@link IQueueMessage#qId()}
     *
     * @param msg
     * @return
     */
    protected boolean putToQueue(IQueueMessage<ID, DATA> msg) {
        byte[] msgData = serialize(msg);
        Object pKey = msg instanceof IPartitionSupport ? ((IPartitionSupport) msg).getPartitionKey()
                : msg.getId();
        if (pKey == null) {
            pKey = msg.getId();
        }
        KafkaMessage kMsg = pKey != null ? new KafkaMessage(topicName, pKey.toString(), msgData)
                : new KafkaMessage(topicName, msgData);
        if (sendAsync) {
            return kafkaClient.sendMessageRaw(producerType, kMsg) != null;
        } else {
            return kafkaClient.sendMessage(producerType, kMsg) != null;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean queue(IQueueMessage<ID, DATA> _msg) {
        IQueueMessage<ID, DATA> msg = _msg.clone();
        Date now = new Date();
        msg.setNumRequeues(0).setQueueTimestamp(now).setTimestamp(now);
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeue(final IQueueMessage<ID, DATA> _msg) {
        IQueueMessage<ID, DATA> msg = _msg.clone();
        Date now = new Date();
        msg.incNumRequeues().setQueueTimestamp(now);
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeueSilent(IQueueMessage<ID, DATA> _msg) {
        IQueueMessage<ID, DATA> msg = _msg.clone();
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage<ID, DATA> msg) {
        // EMPTY
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IQueueMessage<ID, DATA> take() {
        return takeFromQueue();
    }

    /**
     * {@inheritDoc}
     *
     * This method throws {@link QueueException.OperationNotSupported}
     */
    @Override
    public Collection<IQueueMessage<ID, DATA>> getOrphanMessages(long thresholdTimestampMs) {
        throw new QueueException.OperationNotSupported(
                "This queue does not support retrieving orphan messages");
    }

    // /**
    // * {@inheritDoc}
    // */
    // @Override
    // public boolean moveFromEphemeralToQueueStorage(IQueueMessage<ID, DATA>
    // msg) {
    // throw new QueueException.OperationNotSupported(
    // "This queue does not support ephemeral storage.");
    // }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        return SIZE_NOT_SUPPORTED;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        return SIZE_NOT_SUPPORTED;
    }

}

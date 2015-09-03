package com.github.ddth.queue.impl;

import java.util.Collection;
import java.util.Date;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaClient.ProducerType;
import com.github.ddth.kafka.KafkaMessage;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;

/**
 * (Experimental) Kafka implementation of {@link IQueue}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.2
 */
public abstract class KafkaQueue implements IQueue {

    private KafkaClient kafkaClient;
    private boolean myOwnKafkaClient = true;

    private String zkConnString = "localhost:2181/kafka";

    private String topicName = "ddth-queue";

    private ProducerType producerType = ProducerType.SYNC_LEADER_ACK;

    public ProducerType getProducerType() {
        return producerType;
    }

    public KafkaQueue setProducerType(ProducerType producerType) {
        this.producerType = producerType;
        return this;
    }

    /**
     * Zookeeper connection string (format
     * {@code host1:port1,host2:port2,host3:port3/path}).
     * 
     * @return
     */
    public String getZkConnString() {
        return zkConnString;
    }

    /**
     * Sets Zookeeper connection string (format
     * {@code host1:port1,host2:port2,host3:port3/path}).
     * 
     * @param redisHostAndPort
     * @return
     */
    public KafkaQueue setZkConnString(String zkConnString) {
        this.zkConnString = zkConnString;
        return this;
    }

    public String getTopicName() {
        return topicName;
    }

    public KafkaQueue setTopicName(String topicName) {
        this.topicName = topicName;
        return this;
    }

    protected KafkaClient getKafkaClient() {
        return kafkaClient;
    }

    public KafkaQueue setKafkaClient(KafkaClient kafkaClient) {
        this.kafkaClient = kafkaClient;
        myOwnKafkaClient = false;
        return this;
    }

    /*----------------------------------------------------------------------*/

    /**
     * Init method.
     * 
     * @return
     * @throws Exception
     */
    public KafkaQueue init() throws Exception {
        if (kafkaClient == null) {
            kafkaClient = new KafkaClient(zkConnString);
            kafkaClient.init();
            myOwnKafkaClient = true;
        }
        return this;
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        if (kafkaClient != null && myOwnKafkaClient) {
            kafkaClient.destroy();
            kafkaClient = null;
        }
    }

    /**
     * Serializes a queue message to store in Kafka.
     * 
     * @param msg
     * @return
     */
    protected abstract byte[] serialize(IQueueMessage msg);

    /**
     * Deserilizes a queue message.
     * 
     * @param msgData
     * @return
     */
    protected abstract IQueueMessage deserialize(byte[] msgData);

    /**
     * Puts a message to Kafka queue.
     * 
     * @param msg
     * @return
     */
    protected boolean putToQueue(IQueueMessage msg) {
        byte[] msgData = serialize(msg);
        KafkaMessage kmsg = new KafkaMessage(topicName, msg.qId().toString(), msgData);
        kafkaClient.sendMessage(producerType, kmsg);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean queue(IQueueMessage msg) {
        Date now = new Date();
        msg.qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now);
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeue(IQueueMessage msg) {
        Date now = new Date();
        msg.qIncNumRequeues().qTimestamp(now);
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeueSilent(IQueueMessage msg) {
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage msg) {
        // TODO
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IQueueMessage take() {
        throw new UnsupportedOperationException("Method [take] has not implemented yet!");
    }

    /**
     * {@inheritDoc}
     * 
     * @param thresholdTimestampMs
     * @return
     */
    @Override
    public Collection<IQueueMessage> getOrphanMessages(long thresholdTimestampMs) {
        throw new UnsupportedOperationException(
                "Method [getOrphanMessages] has not implemented yet!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean moveFromEphemeralToQueueStorage(IQueueMessage msg) {
        throw new UnsupportedOperationException(
                "Method [moveFromEphemeralToQueueStorage] has not implemented yet!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        return -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        return -1;
    }
}

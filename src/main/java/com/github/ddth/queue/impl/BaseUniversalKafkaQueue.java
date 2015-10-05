package com.github.ddth.queue.impl;

import com.github.ddth.kafka.KafkaMessage;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;

/**
 * Base class for universal Kafka queue implementation.
 * 
 * @author Thanh Nguyen
 * 
 * @param <T>
 * @since 0.3.3
 */
public abstract class BaseUniversalKafkaQueue<T extends BaseUniversalQueueMessage> extends
        KafkaQueue {

    /**
     * Puts a message to Kafka queue, partitioning message by
     * {@link BaseUniversalQueueMessage#partitionKey()} (or
     * {@link IQueueMessage#qId()} if message is not of type
     * {@link BaseUniversalQueueMessage)}).
     * 
     * @param msg
     * @return
     * @since 0.3.3.2
     */
    @Override
    protected boolean putToQueue(IQueueMessage msg) {
        byte[] msgData = serialize(msg);
        Object qId = msg.qId();
        String kafkaKey = msg instanceof BaseUniversalQueueMessage ? ((BaseUniversalQueueMessage) msg)
                .partitionKey() : null;
        kafkaKey = kafkaKey != null ? kafkaKey : (qId != null ? qId.toString() : null);
        KafkaMessage kmsg = kafkaKey != null ? new KafkaMessage(getTopicName(), kafkaKey, msgData)
                : new KafkaMessage(getTopicName(), msgData);
        getKafkaClient().sendMessage(getProducerType(), kmsg);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected byte[] serialize(IQueueMessage _msg) throws QueueException {
        if (_msg == null) {
            return null;
        }
        if (!(_msg instanceof BaseUniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + BaseUniversalQueueMessage.class.getName() + "]!");
        }

        BaseUniversalQueueMessage msg = (BaseUniversalQueueMessage) _msg;
        try {
            return msg.toBytes();
        } catch (Exception e) {
            throw new QueueException.CannotSerializeQueueMessage(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T take() {
        return (T) super.take();
    }
}

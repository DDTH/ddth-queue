package com.github.ddth.queue.impl;

import java.util.Date;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.dao.BaseBo;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueUtils;

/**
 * Universal queue message.
 * 
 * <p>
 * Fields:
 * </p>
 * <ul>
 * <li>{@code queue_id}: see {@link IQueueMessage#qId()}</li>
 * <li>{@code org_timestamp (type: java.util.Date)}: see
 * {@link IQueueMessage#qOriginalTimestamp()}</li>
 * <li>{@code timestamp (type: java.util.Date)}: see
 * {@link IQueueMessage#qTimestamp()}</li>
 * <li>{@code num_requeues (type: int)}: see
 * {@link IQueueMessage#qNumRequeues()}</li>
 * <li>{@code content (type: byte[])}: message's content</li>
 * <li>{@code kafkaKey (type: string)}: key for partitioning Kafka messages, see
 * {@link #kafkaKey()}</li>
 * </ul>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.3
 */
public abstract class BaseUniversalQueueMessage extends BaseBo implements IQueueMessage {

    public final static String FIELD_QUEUE_ID = "queue_id";
    public final static String FIELD_ORG_TIMESTAMP = "org_timestamp";
    public final static String FIELD_TIMESTAMP = "timestamp";
    public final static String FIELD_NUM_REQUEUES = "num_requeues";
    public final static String FIELD_CONTENT = "content";

    /**
     * For Kafka use only.
     * 
     * @since 0.3.3.1
     */
    public final static String FIELD_KAFKA_KEY = "_key_";

    /**
     * Gets Kafka message's key.
     * 
     * @return
     * @since 0.3.3.1
     */
    public String kafkaKey() {
        return getAttribute(FIELD_KAFKA_KEY, String.class);
    }

    /**
     * Sets Kafka message's key.
     * 
     * @param key
     * @return
     * @since 0.3.3.1
     */
    public BaseUniversalQueueMessage kafkaKey(String key) {
        setAttribute(FIELD_KAFKA_KEY, key);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date qOriginalTimestamp() {
        return getAttribute(FIELD_ORG_TIMESTAMP, Date.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseUniversalQueueMessage qOriginalTimestamp(Date timestamp) {
        return (BaseUniversalQueueMessage) setAttribute(FIELD_ORG_TIMESTAMP, timestamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date qTimestamp() {
        return getAttribute(FIELD_TIMESTAMP, Date.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseUniversalQueueMessage qTimestamp(Date timestamp) {
        return (BaseUniversalQueueMessage) setAttribute(FIELD_TIMESTAMP, timestamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int qNumRequeues() {
        Integer value = getAttribute(FIELD_NUM_REQUEUES, Integer.class);
        return value != null ? value.intValue() : 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseUniversalQueueMessage qNumRequeues(int numRequeues) {
        return (BaseUniversalQueueMessage) setAttribute(FIELD_NUM_REQUEUES, numRequeues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    synchronized public BaseUniversalQueueMessage qIncNumRequeues() {
        return qNumRequeues(qNumRequeues() + 1);
    }

    /**
     * Gets message's content.
     * 
     * @return
     */
    public byte[] content() {
        return getAttribute(FIELD_CONTENT, byte[].class);
    }

    /**
     * Gets message's content as a String.
     * 
     * @return
     */
    public String contentAsString() {
        byte[] data = content();
        return data != null ? new String(data, QueueUtils.UTF8) : null;
    }

    /**
     * Sets message's content.
     * 
     * @param content
     * @return
     */
    public BaseUniversalQueueMessage content(byte[] content) {
        return (BaseUniversalQueueMessage) setAttribute(FIELD_CONTENT, content);
    }

    /**
     * Sets message's content.
     * 
     * @param content
     * @return
     */
    public BaseUniversalQueueMessage content(String content) {
        return content(content != null ? content.getBytes(QueueUtils.UTF8) : null);
    }

    /**
     * Serializes to {@code byte[]}.
     * 
     * @return
     */
    public byte[] toBytes() {
        Map<String, Object> dataMap = this.toMap();
        byte[] content = this.content();
        String contentStr = content != null ? Base64.encodeBase64String(content) : null;
        dataMap.put(FIELD_CONTENT, contentStr);
        return SerializationUtils.toJsonString(dataMap).getBytes(QueueUtils.UTF8);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseUniversalQueueMessage fromJson(String jsonString) {
        super.fromJson(jsonString);
        Object content = getAttribute(FIELD_CONTENT);
        if (content != null && !(content instanceof byte[])) {
            String strContent = content.toString();
            content(Base64.decodeBase64(strContent));
        }
        return this;
    }

    /**
     * Deserializes from a {@code byte[]} - which has been serialized by
     * {@link #toBytes()}.
     * 
     * @param msgData
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    @SuppressWarnings("unchecked")
    public static <T extends BaseUniversalQueueMessage> T fromBytes(byte[] msgData, Class<T> clazz)
            throws InstantiationException, IllegalAccessException {
        if (msgData == null) {
            return null;
        }
        String msgDataJson = new String(msgData, QueueUtils.UTF8);
        Map<String, Object> dataMap = SerializationUtils.fromJsonString(msgDataJson, Map.class);
        Object content = dataMap.get(BaseUniversalQueueMessage.FIELD_CONTENT);
        byte[] contentData = content == null ? null : (content instanceof byte[] ? (byte[]) content
                : Base64.decodeBase64(content.toString()));
        dataMap.put(BaseUniversalQueueMessage.FIELD_CONTENT, contentData);
        T msg = clazz.newInstance();
        msg.fromMap(dataMap);
        return msg;
    }
}

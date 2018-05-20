package com.github.ddth.queue.impl.universal.base;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.github.ddth.commons.serialization.ISerializationSupport;
import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.queue.impl.GenericQueueMessage;
import com.github.ddth.queue.utils.QueueUtils;

/**
 * Base class for universal queue messages, where data is stored as
 * {@code byte[]}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.3
 */
public abstract class BaseUniversalQueueMessage<ID> extends GenericQueueMessage<ID, byte[]>
        implements ISerializationSupport {

    /**
     * {@inheritDoc}
     * 
     * @since 0.5.0
     */
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(19, 81);
        hcb.append(getId());
        return hcb.hashCode();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.5.0
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof BaseUniversalQueueMessage) {
            BaseUniversalQueueMessage<?> msg = (BaseUniversalQueueMessage<?>) obj;
            EqualsBuilder eq = new EqualsBuilder();
            eq.append(this.getId(), msg.getId());
            return eq.isEquals();
        }
        return false;
    }

    public final static String FIELD_QUEUE_ID = "id", FIELD_TIMESTAMP = "time",
            FIELD_QUEUE_TIMESTAMP = "queue_time", FIELD_NUM_REQUEUES = "num_requeues",
            FIELD_DATA = "data", FIELD_PARTITION_KEY = "pkey";

    /**
     * Serialize this queue message to a {@link Map}.
     * 
     * @return
     * @since 0.5.0
     */
    public Map<String, Object> toMap() {
        return new HashMap<String, Object>() {
            private static final long serialVersionUID = 1L;
            {
                put(FIELD_QUEUE_ID, getId());
                put(FIELD_TIMESTAMP, getTimestamp());
                put(FIELD_QUEUE_TIMESTAMP, getQueueTimestamp());
                put(FIELD_NUM_REQUEUES, getNumRequeues());
                put(FIELD_DATA, getData());
                put(FIELD_PARTITION_KEY, getPartitionKey());
            }
        };
    }

    /**
     * Deserialize queue message from a {@link Map}.
     * 
     * @param dataMap
     * @return
     * @since 0.5.0
     */
    @SuppressWarnings("unchecked")
    public BaseUniversalQueueMessage<ID> fromMap(Map<String, Object> dataMap) {
        Object queueId = DPathUtils.getValue(dataMap, FIELD_QUEUE_ID);
        if (queueId != null) {
            setId((ID) queueId);
        }

        Date timestamp = DPathUtils.getValue(dataMap, FIELD_TIMESTAMP, Date.class);
        if (timestamp != null) {
            setTimestamp(timestamp);
        }

        Date queueTimestamp = DPathUtils.getValue(dataMap, FIELD_QUEUE_TIMESTAMP, Date.class);
        if (queueTimestamp != null) {
            setQueueTimestamp(queueTimestamp);
        }

        Integer numRequeues = DPathUtils.getValue(dataMap, FIELD_NUM_REQUEUES, Integer.class);
        if (numRequeues != null) {
            setNumRequeues(numRequeues.intValue());
        }

        Object content = DPathUtils.getValue(dataMap, FIELD_DATA);
        if (content != null) {
            if (content instanceof byte[]) {
                setContent((byte[]) content);
            } else if (content instanceof String) {
                setContent((byte[]) Base64.decodeBase64((String) content));
            }
        }

        String partitionKey = DPathUtils.getValue(dataMap, FIELD_PARTITION_KEY, String.class);
        if (partitionKey != null) {
            setPartitionKey(partitionKey);
        }

        return this;
    }

    /**
     * Serialize this queue message to Json string.
     * 
     * @return
     * @since 0.5.0
     */
    protected String toJson() {
        Map<String, Object> dataMap = toMap();
        return dataMap != null ? SerializationUtils.toJsonString(dataMap) : null;
    }

    /**
     * Deserialize queue message from a Json string.
     * 
     * @param dataJson
     * @return
     * @since 0.5.0
     */
    @SuppressWarnings("unchecked")
    protected BaseUniversalQueueMessage<ID> fromJson(String dataJson) {
        Map<String, Object> dataMap = SerializationUtils.fromJsonString(dataJson, Map.class);
        return fromMap(dataMap);
    }

    /**
     * Serializes to {@code byte[]}.
     * 
     * @return
     */
    @Override
    public byte[] toBytes() {
        String json = toJson();
        return json != null ? json.getBytes(QueueUtils.UTF8) : null;
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
    public static <T extends BaseUniversalQueueMessage<ID>, ID> T fromBytes(byte[] msgData,
            Class<T> clazz) throws InstantiationException, IllegalAccessException {
        // firstly, deserialize the input data to a map
        String msgDataJson = msgData != null ? new String(msgData, QueueUtils.UTF8) : null;
        Map<String, Object> dataMap = null;
        try {
            dataMap = msgDataJson != null
                    ? SerializationUtils.fromJsonString(msgDataJson, Map.class) : null;
        } catch (Exception e) {
            dataMap = null;
        }
        if (dataMap == null) {
            return null;
        }
        T msg = clazz.newInstance();
        msg.fromMap(dataMap);
        return msg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseUniversalQueueMessage<ID> clone() {
        return (BaseUniversalQueueMessage<ID>) super.clone();
    }

    /**
     * Set queue message's data.
     *
     * @param data
     * @return
     * @since 0.6.0
     * @deprecated since v0.7.0 use {@link #setContent(String)}
     */
    public BaseUniversalQueueMessage<ID> qData(String data) {
        return content(data);
    }

    /**
     * Gets message's content.
     *
     * @return
     * @deprecated since v0.7.0 use {@link #getContent()}
     */
    public byte[] content() {
        return getContent();
    }

    /**
     * Gets message's content as a String.
     *
     * @return
     * @deprecated since v0.7.0 use {@link #getContentAsString()}
     */
    public String contentAsString() {
        return getContentAsString();
    }

    /**
     * Gets message's content.
     *
     * @return
     * @since 0.7.0
     */
    public byte[] getContent() {
        return getData();
    }

    /**
     * Gets message's content as a String.
     *
     * @return
     * @since 0.7.0
     */
    public String getContentAsString() {
        byte[] data = content();
        return data != null ? new String(data, QueueUtils.UTF8) : null;
    }

    /**
     * Sets message's content.
     * 
     * @param content
     * @return
     * @deprecated since v0.7.0 use {@link #setContent(byte[])}
     */
    public BaseUniversalQueueMessage<ID> content(byte[] content) {
        return setContent(content);
    }

    /**
     * Sets message's content.
     * 
     * @param content
     * @return
     * @deprecated since v0.7.0 use {@link #setContent(String)}
     */
    public BaseUniversalQueueMessage<ID> content(String content) {
        return setContent(content);
    }

    /**
     * Sets message's content.
     * 
     * @param content
     * @return
     * @since 0.7.0
     */
    public BaseUniversalQueueMessage<ID> setContent(byte[] content) {
        setData(content != null ? Arrays.copyOf(content, content.length) : null);
        return this;
    }

    /**
     * Sets message's content.
     * 
     * @param content
     * @return
     * @since 0.7.0
     */
    public BaseUniversalQueueMessage<ID> setContent(String content) {
        return content(content != null ? content.getBytes(QueueUtils.UTF8) : null);
    }

}

package com.github.ddth.pubsub.impl.universal;

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
import com.github.ddth.queue.impl.GenericMessage;
import com.github.ddth.queue.utils.QueueUtils;

/**
 * Base class for universal messages, where data is stored as {@code byte[]}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public abstract class BaseUniversalMessage<ID> extends GenericMessage<ID, byte[]>
        implements ISerializationSupport {

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(19, 81);
        hcb.append(getId());
        return hcb.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj instanceof BaseUniversalMessage) {
            BaseUniversalMessage<?> msg = (BaseUniversalMessage<?>) obj;
            EqualsBuilder eq = new EqualsBuilder();
            eq.append(this.getId(), msg.getId()).append(this.getContent(), msg.getContent())
                    .append(this.getTimestamp(), msg.getTimestamp())
                    .append(this.getPartitionKey(), msg.getPartitionKey());
            return eq.isEquals();
        }
        return false;
    }

    public final static String FIELD_QUEUE_ID = "id", FIELD_TIMESTAMP = "time", FIELD_DATA = "data",
            FIELD_PARTITION_KEY = "pkey";

    /**
     * Serialize this queue message to a {@link Map}.
     * 
     * @return
     */
    public Map<String, Object> toMap() {
        return new HashMap<String, Object>() {
            private static final long serialVersionUID = 1L;
            {
                put(FIELD_QUEUE_ID, getId());
                put(FIELD_TIMESTAMP, getTimestamp());
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
     */
    @SuppressWarnings("unchecked")
    public BaseUniversalMessage<ID> fromMap(Map<String, Object> dataMap) {
        Object queueId = DPathUtils.getValue(dataMap, FIELD_QUEUE_ID);
        if (queueId != null) {
            setId((ID) queueId);
        }

        Date timestamp = DPathUtils.getValue(dataMap, FIELD_TIMESTAMP, Date.class);
        if (timestamp != null) {
            setTimestamp(timestamp);
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
     */
    @SuppressWarnings("unchecked")
    protected BaseUniversalMessage<ID> fromJson(String dataJson) {
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
    public static <T extends BaseUniversalMessage<ID>, ID> T fromBytes(byte[] msgData,
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
    public BaseUniversalMessage<ID> clone() {
        return (BaseUniversalMessage<ID>) super.clone();
    }

    /**
     * Gets message's content.
     * 
     * @return
     */
    public byte[] getContent() {
        return getData();
    }

    /**
     * Gets message's content as a String.
     * 
     * @return
     */
    public String getContentAsString() {
        byte[] data = getContent();
        return data != null ? new String(data, QueueUtils.UTF8) : null;
    }

    /**
     * Sets message's content.
     * 
     * @param content
     * @return
     */
    public BaseUniversalMessage<ID> setContent(byte[] content) {
        setData(content != null ? Arrays.copyOf(content, content.length) : null);
        return this;
    }

    /**
     * Sets message's content.
     * 
     * @param content
     * @return
     */
    public BaseUniversalMessage<ID> setContent(String content) {
        return setContent(content != null ? content.getBytes(QueueUtils.UTF8) : null);
    }
}

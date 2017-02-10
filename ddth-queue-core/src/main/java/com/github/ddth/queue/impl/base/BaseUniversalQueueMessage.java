package com.github.ddth.queue.impl.base;

import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.queue.IPartitionSupport;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueUtils;

/**
 * Base class for universal queue messages.
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
 * <li>{@code partitionKey (type: string)}: key for partitioning messages, see
 * {@link #qPartitionKey()}</li>
 * </ul>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.3
 */
public abstract class BaseUniversalQueueMessage
        implements IQueueMessage, IPartitionSupport, Cloneable {

    public final static String FIELD_QUEUE_ID = "qid";
    public final static String FIELD_ORG_TIMESTAMP = "orgt";
    public final static String FIELD_TIMESTAMP = "t";
    public final static String FIELD_NUM_REQUEUES = "numq";
    public final static String FIELD_CONTENT = "data";

    /**
     * Key used for partitioning messages.
     *
     * @since 0.3.3.2
     */
    protected final static String FIELD_PARTITION_KEY = "_pkey_";

    /**
     * {@inheritDoc}
     * 
     * @since 0.5.0
     */
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(19, 81);
        hcb.append(queueId);
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
            BaseUniversalQueueMessage msg = (BaseUniversalQueueMessage) obj;
            EqualsBuilder eq = new EqualsBuilder();
            eq.append(this.queueId, msg.queueId);
            return eq.isEquals();
        }
        return false;
    }

    /**
     * 
     * @return
     * @since 0.5.0
     */
    public Map<String, Object> toMap() {
        return new HashMap<String, Object>() {
            private static final long serialVersionUID = 1L;
            {
                put(FIELD_QUEUE_ID, queueId);
                put(FIELD_ORG_TIMESTAMP, orgTimestamp);
                put(FIELD_TIMESTAMP, timestamp);
                put(FIELD_NUM_REQUEUES, numRequeues);
                put(FIELD_CONTENT, content);
                put(FIELD_PARTITION_KEY, partitionKey);
            }
        };
    }

    /**
     * 
     * @param dataMap
     * @return
     * @since 0.5.0
     */
    public BaseUniversalQueueMessage fromMap(Map<String, Object> dataMap) {
        Object queueId = DPathUtils.getValue(dataMap, FIELD_QUEUE_ID);
        if (queueId != null) {
            qId(queueId);
        }

        Date orgTimestamp = DPathUtils.getValue(dataMap, FIELD_ORG_TIMESTAMP, Date.class);
        if (orgTimestamp != null) {
            qOriginalTimestamp(orgTimestamp);
        }

        Date timestamp = DPathUtils.getValue(dataMap, FIELD_TIMESTAMP, Date.class);
        if (timestamp != null) {
            qTimestamp(timestamp);
        }

        Integer numRequeues = DPathUtils.getValue(dataMap, FIELD_NUM_REQUEUES, Integer.class);
        if (numRequeues != null) {
            qNumRequeues(numRequeues.intValue());
        }

        Object content = DPathUtils.getValue(dataMap, FIELD_CONTENT);
        if (content != null) {
            if (content instanceof byte[]) {
                qData(content);
            } else if (content instanceof String) {
                qData(Base64.decodeBase64((String) content));
            }
        }

        String partitionKey = DPathUtils.getValue(dataMap, FIELD_PARTITION_KEY, String.class);
        if (partitionKey != null) {
            partitionKey(partitionKey);
        }

        return this;
    }

    /**
     * 
     * @return
     * @since 0.5.0
     */
    protected String toJson() {
        Map<String, Object> dataMap = toMap();
        return dataMap != null ? SerializationUtils.toJsonString(dataMap) : null;
    }

    /**
     * 
     * @param dataJson
     * @return
     * @since 0.5.0
     */
    @SuppressWarnings("unchecked")
    protected BaseUniversalQueueMessage fromJson(String dataJson) {
        Map<String, Object> dataMap = SerializationUtils.fromJsonString(dataJson, Map.class);
        return fromMap(dataMap);
    }

    /**
     * Serializes to {@code byte[]}.
     * 
     * @return
     */
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
    public static <T extends BaseUniversalQueueMessage> T fromBytes(byte[] msgData, Class<T> clazz)
            throws InstantiationException, IllegalAccessException {
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

    private Object queueId;
    private Date orgTimestamp, timestamp;
    volatile private int numRequeues;
    private byte[] content;
    private String partitionKey;

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseUniversalQueueMessage clone() {
        try {
            return (BaseUniversalQueueMessage) super.clone();
        } catch (CloneNotSupportedException e) {
            // should not happen
            throw new RuntimeException(e);
        }
    }

    /**
     * Key used for partitioning messages (some queue implementations, such as
     * Kafka queue) support message partitioning.
     * 
     * @return
     * @since 0.3.3.2
     * @deprecated since 0.5.0, use {@link #qPartitionKey()} instead
     */
    public String partitionKey() {
        return partitionKey;
    }

    /**
     * Key used for partitioning messages (some queue implementations, such as
     * Kafka queue) support message partitioning.
     * 
     * @return
     * @since 0.5.0
     */
    @Override
    public String qPartitionKey() {
        return partitionKey;
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.5.0
     */
    @Override
    public Object qId() {
        return queueId;
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.5.0
     */
    @Override
    public BaseUniversalQueueMessage qId(Object qId) {
        this.queueId = qId;
        return this;
    }

    /**
     * Key used for partitioning messages (some queue implementations, such as
     * Kafka queue) support message partitioning.
     * 
     * @param partitionKey
     * @return
     * @since 0.3.3.2
     * @deprecated since 0.5.0, use {@link #qPartitionKey(String)} instead
     */
    public BaseUniversalQueueMessage partitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
        return this;
    }

    /**
     * Key used for partitioning messages (some queue implementations, such as
     * Kafka queue) support message partitioning.
     * 
     * @param partitionKey
     * @return
     * @since 0.5.0
     */
    @Override
    public BaseUniversalQueueMessage qPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date qOriginalTimestamp() {
        return orgTimestamp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseUniversalQueueMessage qOriginalTimestamp(Date timestamp) {
        this.orgTimestamp = timestamp;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date qTimestamp() {
        return timestamp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseUniversalQueueMessage qTimestamp(Date timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int qNumRequeues() {
        return numRequeues;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseUniversalQueueMessage qNumRequeues(int numRequeues) {
        this.numRequeues = numRequeues < 0 ? 0 : numRequeues;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseUniversalQueueMessage qIncNumRequeues() {
        numRequeues++;
        return this;
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.4.2
     */
    public byte[] qData() {
        return content();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.4.2
     */
    public BaseUniversalQueueMessage qData(Object data) {
        if (data instanceof byte[]) {
            return content((byte[]) data);
        }
        if (data instanceof String) {
            return content((String) data);
        }
        if (data == null) {
            return content((byte[]) null);
        }
        return content(data.toString());
    }

    /**
     * Gets message's content.
     * 
     * @return
     */
    public byte[] content() {
        return content;
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
        this.content = content != null ? Arrays.copyOf(content, content.length) : null;
        return this;
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

}

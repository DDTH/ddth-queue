package com.github.ddth.queue;

import java.util.Date;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.github.ddth.commons.utils.IdGenerator;
import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.dao.BaseBo;
import com.github.ddth.queue.utils.QueueUtils;

/**
 * Universal queue message.
 * 
 * <p>
 * Fields:
 * </p>
 * <ul>
 * <li>{@code queue_id (long)}: see {@link IQueueMessage#qId()}</li>
 * <li>{@code org_timestamp}: see {@link IQueueMessage#qOriginalTimestamp()}</li>
 * <li>{@code timestamp}: see {@link IQueueMessage#qTimestamp()}</li>
 * <li>{@code num_requeues}: see {@link IQueueMessage#qNumRequeues()}</li>
 * <li>{@code content (byte[])}: message's content</li>
 * </ul>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.2.2
 */
public class UniversalQueueMessage extends BaseBo implements IQueueMessage {

    private static IdGenerator IDGEN = IdGenerator.getInstance(IdGenerator.getMacAddr());

    /**
     * Creates a new {@link UniversalQueueMessage} object.
     * 
     * @return
     */
    public static UniversalQueueMessage newInstance() {
        Date now = new Date();
        UniversalQueueMessage msg = new UniversalQueueMessage();
        msg.qId(IDGEN.generateId64()).qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now);
        return msg;
    }

    /**
     * @since v0.2.2.2
     */
    public final static String FIELD_QUEUE_ID = "queue_id";

    /**
     * @since v0.2.2.2
     */
    public final static String FIELD_ORG_TIMESTAMP = "org_timestamp";

    /**
     * @since v0.2.2.2
     */
    public final static String FIELD_TIMESTAMP = "timestamp";

    /**
     * @since v0.2.2.2
     */
    public final static String FIELD_NUM_REQUEUES = "num_requeues";

    /**
     * @since v0.2.2.2
     */
    public final static String FIELD_CONTENT = "content";

    private final static Long ZERO = new Long(0);

    /**
     * {@inheritDoc}
     */
    @Override
    public Long qId() {
        Long value = getAttribute(FIELD_QUEUE_ID, Long.class);
        return value != null ? value : ZERO;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalQueueMessage qId(Object queueId) {
        long value = (queueId instanceof Number) ? ((Number) queueId).longValue() : 0;
        return (UniversalQueueMessage) setAttribute(FIELD_QUEUE_ID, value);
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
    public UniversalQueueMessage qOriginalTimestamp(Date timestamp) {
        return (UniversalQueueMessage) setAttribute(FIELD_ORG_TIMESTAMP, timestamp);
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
    public UniversalQueueMessage qTimestamp(Date timestamp) {
        return (UniversalQueueMessage) setAttribute(FIELD_TIMESTAMP, timestamp);
    }

    @Override
    public int qNumRequeues() {
        Integer value = getAttribute(FIELD_NUM_REQUEUES, Integer.class);
        return value != null ? value.intValue() : 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalQueueMessage qNumRequeues(int numRequeues) {
        return (UniversalQueueMessage) setAttribute(FIELD_NUM_REQUEUES, numRequeues);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalQueueMessage qIncNumRequeues() {
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
     * Sets message's content.
     * 
     * @param content
     * @return
     */
    public UniversalQueueMessage content(byte[] content) {
        return (UniversalQueueMessage) setAttribute(FIELD_CONTENT, content);
    }

    /**
     * Serializes to {@code byte[]}.
     * 
     * @return
     * @since 0.3.2
     */
    public byte[] toBytes() {
        Map<String, Object> dataMap = this.toMap();
        byte[] content = this.content();
        String contentStr = content != null ? Base64.encodeBase64String(content) : null;
        dataMap.put(FIELD_CONTENT, contentStr);
        return SerializationUtils.toJsonString(dataMap).getBytes(QueueUtils.UTF8);
    }

    /**
     * Deserializes from a {@code byte[]}.
     * 
     * @param msgData
     * @return
     * @since 0.3.2
     */
    @SuppressWarnings("unchecked")
    public static UniversalQueueMessage fromBytes(byte[] msgData) {
        if (msgData == null) {
            return null;
        }
        String msgDataJson = new String(msgData, QueueUtils.UTF8);
        Map<String, Object> dataMap = SerializationUtils.fromJsonString(msgDataJson, Map.class);
        Object content = dataMap.get(UniversalQueueMessage.FIELD_CONTENT);
        byte[] contentData = content == null ? null : (content instanceof byte[] ? (byte[]) content
                : Base64.decodeBase64(content.toString()));
        dataMap.put(UniversalQueueMessage.FIELD_CONTENT, contentData);
        UniversalQueueMessage msg = new UniversalQueueMessage();
        msg.fromMap(dataMap);
        return msg;
    }
}

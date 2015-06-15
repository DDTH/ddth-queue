package com.github.ddth.queue;

import java.util.Date;

import com.github.ddth.dao.BaseBo;

/**
 * Universal queue message.
 * 
 * <p>
 * Fields:
 * </p>
 * <ul>
 * <li>{@code queue_id}: see {@link IQueueMessage#qId()}</li>
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

    /**
     * Creates a new {@link UniversalQueueMessage} object.
     * 
     * @return
     */
    public static UniversalQueueMessage newInstance() {
        Date now = new Date();
        UniversalQueueMessage msg = new UniversalQueueMessage();
        msg.qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now);
        return msg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object qId() {
        Long value = getAttribute("queue_id", Long.class);
        return value != null ? value.longValue() : 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalQueueMessage qId(Object queueId) {
        long value = (queueId instanceof Number) ? ((Number) queueId).longValue() : 0;
        return (UniversalQueueMessage) setAttribute("queue_id", value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date qOriginalTimestamp() {
        return getAttribute("org_timestamp", Date.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalQueueMessage qOriginalTimestamp(Date timestamp) {
        return (UniversalQueueMessage) setAttribute("org_timestamp", timestamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date qTimestamp() {
        return getAttribute("timestamp", Date.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalQueueMessage qTimestamp(Date timestamp) {
        return (UniversalQueueMessage) setAttribute("timestamp", timestamp);
    }

    @Override
    public int qNumRequeues() {
        Integer value = getAttribute("num_requeues", Integer.class);
        return value != null ? value.intValue() : 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalQueueMessage qNumRequeues(int numRequeues) {
        return (UniversalQueueMessage) setAttribute("num_requeues", numRequeues);
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
        return getAttribute("content", byte[].class);
    }

    /**
     * Sets message's content.
     * 
     * @param content
     * @return
     */
    public UniversalQueueMessage content(byte[] content) {
        return (UniversalQueueMessage) setAttribute("content", content);
    }
}

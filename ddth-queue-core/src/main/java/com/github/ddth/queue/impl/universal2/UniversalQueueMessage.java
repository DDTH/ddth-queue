package com.github.ddth.queue.impl.universal2;

import java.util.Date;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.queue.impl.base.BaseUniversalQueueMessage;
import com.github.ddth.queue.utils.QueueUtils;

/**
 * Extended from {@link BaseUniversalQueueMessage} where {@code queue_id} is a
 * {@code String}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.3
 */
public class UniversalQueueMessage extends BaseUniversalQueueMessage {

    /**
     * Creates a new {@link UniversalQueueMessage} object.
     * 
     * @return
     */
    public static UniversalQueueMessage newInstance() {
        Date now = new Date();
        UniversalQueueMessage msg = new UniversalQueueMessage();
        msg.qId(QueueUtils.IDGEN.generateId128Hex().toLowerCase()).qNumRequeues(0)
                .qOriginalTimestamp(now).qTimestamp(now);
        return msg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalQueueMessage clone() {
        return (UniversalQueueMessage) super.clone();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String qId() {
        Object qId = super.qId();
        return qId != null ? qId.toString() : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalQueueMessage qId(Object queueId) {
        super.qId(queueId != null ? queueId.toString() : null);
        return this;
    }

    /**
     * Deserializes from a {@code byte[]}.
     * 
     * @param msgData
     * @return
     */
    public static UniversalQueueMessage fromBytes(byte[] msgData) {
        try {
            return BaseUniversalQueueMessage.fromBytes(msgData, UniversalQueueMessage.class);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) {
        UniversalQueueMessage msg = UniversalQueueMessage.newInstance();
        msg.content("content".getBytes());

        String json1 = msg.toJson();
        System.out.println("Json: " + json1);
        System.out.println("Content: " + new String(msg.content()));

        byte[] data = msg.toBytes();
        msg = UniversalQueueMessage.fromBytes(data);

        String json2 = msg.toJson();
        System.out.println("Json: " + json2);
        System.out.println("Content: " + new String(msg.content()));

        System.out.println(StringUtils.equals(json1, json2));

        msg.fromJson(json1);
        String json3 = msg.toJson();
        System.out.println("Json: " + json3);
        System.out.println("Content: " + new String(msg.content()));
    }
}

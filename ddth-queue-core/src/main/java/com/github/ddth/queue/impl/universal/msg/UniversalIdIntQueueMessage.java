package com.github.ddth.queue.impl.universal.msg;

import java.util.Date;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.queue.impl.universal.base.BaseUniversalQueueMessage;
import com.github.ddth.queue.utils.QueueUtils;

/**
 * Extended from {@link BaseUniversalQueueMessage} where queue message's id is a
 * {@code long}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.2.2
 */
public class UniversalIdIntQueueMessage extends BaseUniversalQueueMessage<Long> {

    /**
     * Deserializes from a {@code byte[]} - which has been serialized by
     * {@link #toBytes()}.
     *
     * @param msgData
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     * @since 0.6.0
     */
    public static UniversalIdIntQueueMessage fromBytes(byte[] data)
            throws InstantiationException, IllegalAccessException {
        return BaseUniversalQueueMessage.fromBytes(data, UniversalIdIntQueueMessage.class);
    }

    /**
     * Create a new {@link UniversalIdIntQueueMessage} object.
     * 
     * @return
     */
    public static UniversalIdIntQueueMessage newInstance() {
        Date now = new Date();
        UniversalIdIntQueueMessage msg = new UniversalIdIntQueueMessage();
        msg.qId(QueueUtils.IDGEN.generateId64()).qNumRequeues(0).qOriginalTimestamp(now)
                .qTimestamp(now);
        return msg;
    }

    /**
     * Create a new {@link UniversalIdIntQueueMessage} object with specified
     * content.
     * 
     * @param content
     * @return
     * @since 0.6.0
     */
    public static UniversalIdIntQueueMessage newInstance(String content) {
        UniversalIdIntQueueMessage msg = newInstance();
        msg.content(content);
        return msg;
    }

    /**
     * Create a new {@link UniversalIdIntQueueMessage} object with specified
     * content.
     * 
     * @param content
     * @return
     * @since 0.6.0
     */
    public static UniversalIdIntQueueMessage newInstance(byte[] content) {
        UniversalIdIntQueueMessage msg = newInstance();
        msg.content(content);
        return msg;
    }

    /**
     * Create a new {@link UniversalIdIntQueueMessage}.
     * 
     * @param data
     * @return
     * @since 0.6.2.3
     */
    public static UniversalIdIntQueueMessage newInstance(Map<String, Object> data) {
        UniversalIdIntQueueMessage msg = newInstance();
        return msg.fromMap(data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdIntQueueMessage clone() {
        return (UniversalIdIntQueueMessage) super.clone();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdIntQueueMessage fromMap(Map<String, Object> dataMap) {
        return (UniversalIdIntQueueMessage) super.fromMap(dataMap);
    }

    public static void main(String[] args) throws Exception {
        UniversalIdIntQueueMessage msg = UniversalIdIntQueueMessage.newInstance();
        msg.content("content".getBytes());

        String json1 = msg.toJson();
        System.out.println("Json: " + json1);
        System.out.println("Content: " + new String(msg.content()));

        byte[] data = msg.toBytes();
        msg = BaseUniversalQueueMessage.fromBytes(data, UniversalIdIntQueueMessage.class);

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

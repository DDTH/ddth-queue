package com.github.ddth.queue.impl.universal.msg;

import java.util.Date;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.queue.impl.universal.base.BaseUniversalQueueMessage;
import com.github.ddth.queue.utils.QueueUtils;

/**
 * Extended from {@link BaseUniversalQueueMessage} where {@code queue_id} is a
 * {@code String}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.3
 */
public class UniversalIdStrQueueMessage extends BaseUniversalQueueMessage<String> {

    /**
     * Deserializes from a {@code byte[]} - which has been serialized by
     * {@link #toBytes()}.
     *
     * @param msgData
     * @return
     * @throws IllegalAccessException
     * @throws InstantiationException
     */
    public static UniversalIdStrQueueMessage fromBytes(byte[] data)
            throws InstantiationException, IllegalAccessException {
        return BaseUniversalQueueMessage.fromBytes(data, UniversalIdStrQueueMessage.class);
    }

    /**
     * Creates a new {@link UniversalIdStrQueueMessage} object.
     * 
     * @return
     */
    public static UniversalIdStrQueueMessage newInstance() {
        Date now = new Date();
        UniversalIdStrQueueMessage msg = new UniversalIdStrQueueMessage();
        msg.qId(QueueUtils.IDGEN.generateId128Hex().toLowerCase()).qNumRequeues(0)
                .qOriginalTimestamp(now).qTimestamp(now);
        return msg;
    }

    /**
     * Create a new {@link UniversalIdStrQueueMessage} object with specified
     * content.
     * 
     * @param content
     * @return
     * @since 0.6.0
     */
    public static UniversalIdStrQueueMessage newInstance(String content) {
        UniversalIdStrQueueMessage msg = newInstance();
        msg.content(content);
        return msg;
    }

    /**
     * Create a new {@link UniversalIdStrQueueMessage} object with specified
     * content.
     * 
     * @param content
     * @return
     * @since 0.6.0
     */
    public static UniversalIdStrQueueMessage newInstance(byte[] content) {
        UniversalIdStrQueueMessage msg = newInstance();
        msg.content(content);
        return msg;
    }

    /**
     * Create a new {@link UniversalIdStrQueueMessage}.
     * 
     * @param data
     * @return
     * @since 0.6.2.3
     */
    public static UniversalIdStrQueueMessage newInstance(Map<String, Object> data) {
        UniversalIdStrQueueMessage msg = newInstance();
        return msg.fromMap(data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdStrQueueMessage clone() {
        return (UniversalIdStrQueueMessage) super.clone();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdStrQueueMessage fromMap(Map<String, Object> dataMap) {
        return (UniversalIdStrQueueMessage) super.fromMap(dataMap);
    }

    public static void main(String[] args) throws Exception {
        UniversalIdStrQueueMessage msg = UniversalIdStrQueueMessage.newInstance();
        msg.content("content".getBytes());

        String json1 = msg.toJson();
        System.out.println("Json: " + json1);
        System.out.println("Content: " + new String(msg.content()));

        byte[] data = msg.toBytes();
        msg = UniversalIdStrQueueMessage.fromBytes(data);

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

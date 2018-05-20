package com.github.ddth.queue.impl.universal.msg;

import java.util.Date;
import java.util.Map;

import com.github.ddth.queue.impl.universal.base.BaseUniversalQueueMessage;
import com.github.ddth.queue.utils.QueueUtils;

/**
 * Extended from {@link BaseUniversalQueueMessage} where message's id is a
 * {@code String}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.3
 */
public class UniversalIdStrQueueMessage extends BaseUniversalQueueMessage<String> {

    /**
     * {@inheritDoc}
     * 
     * @since 0.7.0
     */
    @Override
    public UniversalIdStrQueueMessage fromBytes(byte[] data) {
        try {
            UniversalIdStrQueueMessage other = BaseUniversalQueueMessage.fromBytes(data,
                    UniversalIdStrQueueMessage.class);
            fromMap(other.toMap());
            return this;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    // /**
    // * Deserializes from a {@code byte[]} - which has been serialized by
    // * {@link #toBytes()}.
    // *
    // * @param msgData
    // * @return
    // * @throws IllegalAccessException
    // * @throws InstantiationException
    // */
    // public static UniversalIdStrQueueMessage fromBytes(byte[] data)
    // throws InstantiationException, IllegalAccessException {
    // return BaseUniversalQueueMessage.fromBytes(data,
    // UniversalIdStrQueueMessage.class);
    // }

    /**
     * Creates a new {@link UniversalIdStrQueueMessage} object.
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public static UniversalIdStrQueueMessage newInstance() {
        Date now = new Date();
        UniversalIdStrQueueMessage msg = new UniversalIdStrQueueMessage();
        msg.setQueueTimestamp(now).setNumRequeues(0)
                .setId(QueueUtils.IDGEN.generateId128Hex().toLowerCase()).setTimestamp(now);
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
        msg.setContent(content);
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
        msg.setContent(content);
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

}

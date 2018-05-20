package com.github.ddth.queue.impl.universal;

import java.util.Date;
import java.util.Map;

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
     * {@inheritDoc}
     * 
     * @since 0.7.0
     */
    @Override
    public UniversalIdIntQueueMessage fromBytes(byte[] data) {
        try {
            UniversalIdIntQueueMessage other = BaseUniversalQueueMessage.fromBytes(data,
                    UniversalIdIntQueueMessage.class);
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
    // * @since 0.6.0
    // */
    // public static UniversalIdIntQueueMessage fromBytes(byte[] data)
    // throws InstantiationException, IllegalAccessException {
    // return BaseUniversalQueueMessage.fromBytes(data,
    // UniversalIdIntQueueMessage.class);
    // }

    /**
     * Create a new {@link UniversalIdIntQueueMessage} object.
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public static UniversalIdIntQueueMessage newInstance() {
        Date now = new Date();
        UniversalIdIntQueueMessage msg = new UniversalIdIntQueueMessage();
        msg.setQueueTimestamp(now).setNumRequeues(0).setId(QueueUtils.IDGEN.generateId64())
                .setTimestamp(now);
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
        msg.setContent(content);
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
        msg.setContent(content);
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
}

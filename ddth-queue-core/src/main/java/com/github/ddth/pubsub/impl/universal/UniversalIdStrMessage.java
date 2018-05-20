package com.github.ddth.pubsub.impl.universal;

import java.util.Date;
import java.util.Map;

import com.github.ddth.queue.utils.QueueUtils;

/**
 * Extended from {@link BaseUniversalMessage} where message's is a
 * {@code String}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class UniversalIdStrMessage extends BaseUniversalMessage<String> {

    /**
     * Creates a new {@link UniversalIdStrMessage} object.
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public static UniversalIdStrMessage newInstance() {
        Date now = new Date();
        UniversalIdStrMessage msg = new UniversalIdStrMessage();
        msg.setId(QueueUtils.IDGEN.generateId128Hex().toLowerCase()).setTimestamp(now);
        return msg;
    }

    /**
     * Create a new {@link UniversalIdStrMessage} object with specified content.
     * 
     * @param content
     * @return
     */
    public static UniversalIdStrMessage newInstance(String content) {
        UniversalIdStrMessage msg = newInstance();
        msg.setContent(content);
        return msg;
    }

    /**
     * Create a new {@link UniversalIdStrMessage} object with specified id and
     * content.
     * 
     * @param id
     * @param content
     * @return
     */
    public static UniversalIdStrMessage newInstance(String id, String content) {
        UniversalIdStrMessage msg = newInstance(content);
        msg.setId(id);
        return msg;
    }

    /**
     * Create a new {@link UniversalIdStrMessage} object with specified content.
     * 
     * @param content
     * @return
     */
    public static UniversalIdStrMessage newInstance(byte[] content) {
        UniversalIdStrMessage msg = newInstance();
        msg.setContent(content);
        return msg;
    }

    /**
     * Create a new {@link UniversalIdStrMessage} object with specified id and
     * content.
     * 
     * @param id
     * @param content
     * @return
     */
    public static UniversalIdStrMessage newInstance(String id, byte[] content) {
        UniversalIdStrMessage msg = newInstance(content);
        msg.setId(id);
        return msg;
    }

    /**
     * Create a new {@link UniversalIdStrMessage}.
     * 
     * @param data
     * @return
     */
    public static UniversalIdStrMessage newInstance(Map<String, Object> data) {
        UniversalIdStrMessage msg = newInstance();
        return msg.fromMap(data);
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdStrMessage clone() {
        return (UniversalIdStrMessage) super.clone();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdStrMessage fromBytes(byte[] data) {
        try {
            UniversalIdStrMessage other = BaseUniversalMessage.fromBytes(data,
                    UniversalIdStrMessage.class);
            fromMap(other.toMap());
            return this;
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdStrMessage fromMap(Map<String, Object> dataMap) {
        return (UniversalIdStrMessage) super.fromMap(dataMap);
    }

}

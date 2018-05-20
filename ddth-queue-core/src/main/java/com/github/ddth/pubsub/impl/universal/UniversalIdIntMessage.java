package com.github.ddth.pubsub.impl.universal;

import java.util.Date;
import java.util.Map;

import com.github.ddth.queue.utils.QueueUtils;

/**
 * Extended from {@link BaseUniversalMessage} where message's id is a
 * {@code long}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0.
 */
public class UniversalIdIntMessage extends BaseUniversalMessage<Long> {

    /**
     * Create a new {@link UniversalIdIntMessage} object.
     * 
     * @return
     */
    @SuppressWarnings("unchecked")
    public static UniversalIdIntMessage newInstance() {
        Date now = new Date();
        UniversalIdIntMessage msg = new UniversalIdIntMessage();
        msg.setId(QueueUtils.IDGEN.generateId64()).setTimestamp(now);
        return msg;
    }

    /**
     * Create a new {@link UniversalIdIntMessage} object with specified content.
     * 
     * @param content
     * @return
     */
    public static UniversalIdIntMessage newInstance(String content) {
        UniversalIdIntMessage msg = newInstance();
        msg.setContent(content);
        return msg;
    }

    /**
     * Create a new {@link UniversalIdIntMessage} object with specified content.
     * 
     * @param content
     * @return
     */
    public static UniversalIdIntMessage newInstance(byte[] content) {
        UniversalIdIntMessage msg = newInstance();
        msg.setContent(content);
        return msg;
    }

    /**
     * Create a new {@link UniversalIdIntMessage}.
     * 
     * @param data
     * @return
     */
    public static UniversalIdIntMessage newInstance(Map<String, Object> data) {
        UniversalIdIntMessage msg = newInstance();
        return msg.fromMap(data);
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdIntMessage clone() {
        return (UniversalIdIntMessage) super.clone();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdIntMessage fromBytes(byte[] data) {
        try {
            UniversalIdIntMessage other = BaseUniversalMessage.fromBytes(data,
                    UniversalIdIntMessage.class);
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
    public UniversalIdIntMessage fromMap(Map<String, Object> dataMap) {
        return (UniversalIdIntMessage) super.fromMap(dataMap);
    }
}

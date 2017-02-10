package com.github.ddth.queue.impl.universal;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.Date;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.queue.impl.base.BaseUniversalQueueMessage;
import com.github.ddth.queue.utils.QueueUtils;

/**
 * Extended from {@link BaseUniversalQueueMessage} where {@code queue_id} is a
 * {@code long}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.2.2
 */
public class UniversalQueueMessage extends BaseUniversalQueueMessage {

    private final static Long ZERO = new Long(0);

    /**
     * Creates a new {@link UniversalQueueMessage} object.
     * 
     * @return
     */
    public static UniversalQueueMessage newInstance() {
        Date now = new Date();
        UniversalQueueMessage msg = new UniversalQueueMessage();
        msg.qId(QueueUtils.IDGEN.generateId64()).qNumRequeues(0).qOriginalTimestamp(now)
                .qTimestamp(now);
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
    public Long qId() {
        Object qId = super.qId();
        if (qId instanceof Number) {
            return ((Number) qId).longValue();
        }
        if (qId instanceof BigInteger) {
            return ((BigInteger) qId).longValue();
        }
        if (qId instanceof BigDecimal) {
            return ((BigDecimal) qId).longValue();
        }
        return ZERO;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalQueueMessage qId(Object qId) {
        if (qId instanceof Number) {
            super.qId(((Number) qId).longValue());
        } else if (qId instanceof BigInteger) {
            super.qId(((BigInteger) qId).longValue());
        } else if (qId instanceof BigDecimal) {
            super.qId(((BigDecimal) qId).longValue());
        } else {
            super.qId(ZERO);
        }
        return this;
    }

    /**
     * Deserializes from a {@code byte[]}.
     *
     * @param msgData
     * @return
     * @since 0.3.2
     */
    public static UniversalQueueMessage fromBytes(byte[] msgData) {
        try {
            return BaseUniversalQueueMessage.fromBytes(msgData, UniversalQueueMessage.class);
        } catch (InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }

    public static void main(String[] args) throws Exception {
        UniversalQueueMessage msg = UniversalQueueMessage.newInstance();
        msg.content("content".getBytes());

        String json1 = msg.toJson();
        System.out.println("Json: " + json1);
        System.out.println("Content: " + new String(msg.content()));

        byte[] data = msg.toBytes();
        msg = BaseUniversalQueueMessage.fromBytes(data, UniversalQueueMessage.class);

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

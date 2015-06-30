package com.github.ddth.queue.impl;

import java.util.Map;

import org.apache.commons.codec.binary.Base64;

import com.github.ddth.commons.utils.SerializationUtils;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.UniversalQueueMessage;

/**
 * Universal Redis implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalQueueMessage}s.
 * </p>
 * 
 * <p>
 * Implementation: see {@link RedisQueue}.
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.0
 */
public class UniversalRedisQueue extends RedisQueue {

    /**
     * {@inheritDoc}
     */
    @Override
    protected byte[] serialize(IQueueMessage _msg) {
        if (_msg == null) {
            return null;
        }
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }

        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        Map<String, Object> dataMap = msg.toMap();
        byte[] content = msg.content();
        String contentStr = content != null ? Base64.encodeBase64String(content) : null;
        dataMap.put(UniversalQueueMessage.FIELD_CONTENT, contentStr);
        return SerializationUtils.toJsonString(dataMap).getBytes(UTF8);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    protected UniversalQueueMessage deserialize(byte[] msgData) {
        if (msgData == null) {
            return null;
        }
        Map<String, Object> dataMap = SerializationUtils.fromJsonString(new String(msgData, UTF8),
                Map.class);
        Object content = dataMap.get(UniversalQueueMessage.FIELD_CONTENT);
        byte[] contentData = content == null ? null : (content instanceof byte[] ? (byte[]) content
                : Base64.decodeBase64(content.toString()));
        dataMap.put(UniversalQueueMessage.FIELD_CONTENT, contentData);
        UniversalQueueMessage msg = new UniversalQueueMessage();
        msg.fromMap(dataMap);
        return msg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalQueueMessage take() {
        return (UniversalQueueMessage) super.take();
    }
}

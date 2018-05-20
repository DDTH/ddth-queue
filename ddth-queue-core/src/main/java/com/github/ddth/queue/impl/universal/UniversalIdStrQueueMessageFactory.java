package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.IQueueMessageFactory;

/**
 * Factory that creates {@link UniversalIdStrQueueMessage}s.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class UniversalIdStrQueueMessageFactory implements IQueueMessageFactory<String, byte[]> {

    public final static UniversalIdStrQueueMessageFactory INSTANCE = new UniversalIdStrQueueMessageFactory();

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdStrQueueMessage createMessage() {
        return UniversalIdStrQueueMessage.newInstance();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdStrQueueMessage createMessage(byte[] data) {
        return UniversalIdStrQueueMessage.newInstance(data);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalIdStrQueueMessage createMessage(String id, byte[] data) {
        return UniversalIdStrQueueMessage.newInstance(id, data);
    }

}

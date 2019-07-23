package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.impl.MongodbQueue;
import com.github.ddth.queue.impl.universal.BaseUniversalMongodbQueue;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessageFactory;

/**
 * (Experimental) Universal MongoDB implementation of {@link IQueue}.
 *
 * <p>
 * Queue and Take {@link UniversalIdStrQueueMessage}s.
 * </p>
 *
 * <p>
 * Implementation: see {@link MongodbQueue}.
 * </p>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.7.1
 */
public class UniversalMongodbQueue extends BaseUniversalMongodbQueue<UniversalIdStrQueueMessage, String> {
    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalMongodbQueue init() throws Exception {
        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdStrQueueMessageFactory.INSTANCE);
        }
        super.init();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdStrQueueMessage deserialize(byte[] msgData) {
        return deserialize(msgData, UniversalIdStrQueueMessage.class);
    }
}

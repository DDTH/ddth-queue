package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.BaseLessLockingUniversalSingleStorageJdbcQueue;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessageFactory;

/**
 * Same as {@link AbstractLessLockingUniversalJdbcQueue}, but messages from all queues are stored in one same storage.
 *
 * <p>
 * Queue and Take {@link UniversalIdStrQueueMessage}s.
 * </p>
 *
 * <p>
 * Queue db table schema:
 * </p>
 * <ul>
 * <li>{@code queue_id}: {@code varchar(32)}, see {@link IQueueMessage#getId()}, {@link #COL_QUEUE_ID}</li>
 * <li>Other fields: see {@link BaseLessLockingUniversalSingleStorageJdbcQueue}</li>
 * </ul>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.0
 */
public class AbstractLessLockingUniversalSingleStorageJdbcQueue
        extends BaseLessLockingUniversalSingleStorageJdbcQueue<UniversalIdStrQueueMessage, String> {
    /**
     * {@inheritDoc}
     *
     * @throws Exception
     * @since 0.6.2.3
     */
    @Override
    public AbstractLessLockingUniversalSingleStorageJdbcQueue init() throws Exception {
        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdStrQueueMessageFactory.INSTANCE);
        }
        super.init();
        return this;
    }
}

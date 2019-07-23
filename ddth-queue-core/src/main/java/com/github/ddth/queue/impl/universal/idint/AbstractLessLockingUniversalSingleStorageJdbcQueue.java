package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.BaseLessLockingUniversalSingleStorageJdbcQueue;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessageFactory;

/**
 * Same as {@link AbstractLessLockingUniversalJdbcQueue}, but messages from all queues are stored in one same storage.
 *
 * <p>
 * Queue and Take {@link UniversalIdIntQueueMessage}s.
 * </p>
 *
 * <p>
 * Queue db table schema:
 * </p>
 * <ul>
 * <li>{@code queue_id}: {@code bigint, should be auto-number}, see {@link IQueueMessage#getId()}, {@link #COL_QUEUE_ID}</li>
 * <li>Other fields: see {@link BaseLessLockingUniversalSingleStorageJdbcQueue}</li>
 * </ul>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.0
 */
public abstract class AbstractLessLockingUniversalSingleStorageJdbcQueue
        extends BaseLessLockingUniversalSingleStorageJdbcQueue<UniversalIdIntQueueMessage, Long> {
    /**
     * {@inheritDoc}
     *
     * @throws Exception
     * @since 0.6.2.3
     */
    @Override
    public AbstractLessLockingUniversalSingleStorageJdbcQueue init() throws Exception {
        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdIntQueueMessageFactory.INSTANCE);
        }
        super.init();
        return this;
    }
}

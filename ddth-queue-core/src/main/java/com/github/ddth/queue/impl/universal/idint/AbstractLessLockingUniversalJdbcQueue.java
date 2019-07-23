package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.BaseLessLockingUniversalJdbcQueue;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessageFactory;

/**
 * Same as {@link UniversalJdbcQueue}, but using a "less-locking" algorithm,
 * requires only one single db table for both queue and ephemeral storage.
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
 * <li>Other fields: see {@link BaseLessLockingUniversalJdbcQueue}</li>
 * </ul>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @see BaseLessLockingUniversalJdbcQueue
 * @since 0.5.1.1
 */
public abstract class AbstractLessLockingUniversalJdbcQueue
        extends BaseLessLockingUniversalJdbcQueue<UniversalIdIntQueueMessage, Long> {
    /**
     * {@inheritDoc}
     *
     * @throws Exception
     * @since 0.6.0
     */
    @Override
    public AbstractLessLockingUniversalJdbcQueue init() throws Exception {
        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdIntQueueMessageFactory.INSTANCE);
        }
        super.init();
        return this;
    }
}

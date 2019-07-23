package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.BaseLessLockingUniversalJdbcQueue;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessageFactory;
import com.github.ddth.queue.impl.universal.idint.UniversalJdbcQueue;

/**
 * Same as {@link UniversalJdbcQueue}, but using a "less-locking" algorithm,
 * requires only one single db table for both queue and ephemeral storage.
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
 * <li>Other fields: see {@link BaseLessLockingUniversalJdbcQueue}</li>
 * </ul>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @see BaseLessLockingUniversalJdbcQueue
 * @since 0.5.1.1
 */
public abstract class AbstractLessLockingUniversalJdbcQueue
        extends BaseLessLockingUniversalJdbcQueue<UniversalIdStrQueueMessage, String> {
    /**
     * {@inheritDoc}
     *
     * @throws Exception
     * @since 0.6.0
     */
    @Override
    public AbstractLessLockingUniversalJdbcQueue init() throws Exception {
        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdStrQueueMessageFactory.INSTANCE);
        }
        super.init();
        return this;
    }
}

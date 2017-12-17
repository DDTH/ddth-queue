package com.github.ddth.queue.impl.universal.idint;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DuplicateKeyException;

import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DuplicatedValueException;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.base.BaseUniversalJdbcQueue;
import com.github.ddth.queue.impl.universal.msg.UniversalIdIntQueueMessage;
import com.github.ddth.queue.utils.QueueException;
import com.github.ddth.queue.utils.QueueUtils;

/**
 * Same as {@link AbstractLessLockingUniversalJdbcQueue} but messages from all
 * queues are stored in one same storage.
 * 
 * <p>
 * Queue db table schema:
 * </p>
 * <ul>
 * <li>{@code queue_name}: {@code string}, queue's name, to group queue
 * messages</li>
 * <li>{@code queue_id}: {@code bigint, auto increment}, see
 * {@link IQueueMessage#qId()}, {@link #COL_QUEUE_ID}</li>
 * <li>{@code ephemeral_id}: {@code bigint}, see {@link #COL_EPHEMERAL_ID}</li>
 * <li>{@code msg_org_timestamp}: {@code datetime}, see
 * {@link IQueueMessage#qOriginalTimestamp()}, {@link #COL_ORG_TIMESTAMP}</li>
 * <li>{@code msg_timestamp}: {@code datetime}, see
 * {@link IQueueMessage#qTimestamp()}, {@link #COL_TIMESTAMP}</li>
 * <li>{@code msg_num_requeues}: {@code int}, see
 * {@link IQueueMessage#qNumRequeues()}, {@link #COL_NUM_REQUEUES}</li>
 * <li>{@code msg_content}: {@code blob}, message's content, see
 * {@link #COL_CONTENT}</li>
 * </ul>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.0
 */
public class AbstractLessLockingUniversalSingleStorageJdbcQueue
        extends BaseUniversalJdbcQueue<UniversalIdIntQueueMessage, Long> {

    private Logger LOGGER = LoggerFactory
            .getLogger(AbstractLessLockingUniversalSingleStorageJdbcQueue.class);

    /** Table's column name to store queue-name */
    public final static String COL_QUEUE_NAME = "queue_name";

    /** Table's column name to store queue-id */
    public final static String COL_QUEUE_ID = "queue_id";

    /** Table's column name to store ephemeral id */
    public final static String COL_EPHEMERAL_ID = "ephemeral_id";

    /** Table's column name to store message's original timestamp */
    public final static String COL_ORG_TIMESTAMP = "msg_org_timestamp";

    /** Table's column name to store message's timestamp */
    public final static String COL_TIMESTAMP = "msg_timestamp";

    /** Table's column name to store message's number of requeues */
    public final static String COL_NUM_REQUEUES = "msg_num_requeues";

    /** Table's column name to store message's content */
    public final static String COL_CONTENT = "msg_content";

    private boolean fifo = true;

    /**
     * When set to {@code true}, queue message with lower id is ensured to be
     * taken first. When set to {@code false}, order of taken queue messages
     * depends on the DBMS (usually FIFO in most cases).
     * 
     * @param fifo
     * @return
     */
    public AbstractLessLockingUniversalSingleStorageJdbcQueue setFifo(boolean fifo) {
        this.fifo = fifo;
        return this;
    }

    /**
     * When set to {@code true}, queue message with lower id is ensured to be
     * taken first. When set to {@code false}, order of taken queue messages
     * depends on the DBMS (usually FIFO in most cases).
     * 
     * @param fifo
     * @return
     */
    public AbstractLessLockingUniversalSingleStorageJdbcQueue markFifo(boolean fifo) {
        this.fifo = fifo;
        return this;
    }

    /**
     * If {@code true}, queue message with lower id is ensured to be taken
     * first. Otherwise, order of taken queue messages depends on the DBMS
     * (usually FIFO in most cases).
     * 
     * @return
     */
    public boolean isFifo() {
        return fifo;
    }

    /**
     * If {@code true}, queue message with lower id is ensured to be taken
     * first. Otherwise, order of taken queue messages depends on the DBMS
     * (usually FIFO in most cases).
     * 
     * @return
     */
    public boolean getFifo() {
        return fifo;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTableNameEphemeral() {
        return getTableName();
    }

    private final static String FIELD_COUNT = "num_entries";
    private String SQL_COUNT = "SELECT COUNT(*) AS " + FIELD_COUNT + " FROM {0} WHERE "
            + COL_QUEUE_NAME + "=? AND " + COL_EPHEMERAL_ID + "=0";
    private String SQL_COUNT_EPHEMERAL = "SELECT COUNT(*) AS " + FIELD_COUNT + " FROM {0} WHERE "
            + COL_QUEUE_NAME + "=? AND " + COL_EPHEMERAL_ID + "!=0";

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.2.3
     */
    @Override
    public AbstractLessLockingUniversalSingleStorageJdbcQueue init() {
        super.init();

        SQL_COUNT = MessageFormat.format(SQL_COUNT, getTableName());
        SQL_COUNT_EPHEMERAL = MessageFormat.format(SQL_COUNT_EPHEMERAL, getTableNameEphemeral());

        return this;
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.2.3
     */
    @Override
    protected int queueSize(Connection conn) {
        Map<String, Object> row = getJdbcHelper().executeSelectOne(conn, SQL_COUNT, getQueueName());
        Integer result = DPathUtils.getValue(row, FIELD_COUNT, Integer.class);
        return result != null ? result.intValue() : 0;
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.2.3
     */
    @Override
    protected int ephemeralSize(Connection conn) {
        Map<String, Object> row = getJdbcHelper().executeSelectOne(conn, SQL_COUNT_EPHEMERAL,
                getQueueName());
        Integer result = DPathUtils.getValue(row, FIELD_COUNT, Integer.class);
        return result != null ? result.intValue() : 0;
    }

    /*----------------------------------------------------------------------*/

    protected String SQL_GET_ORPHAN_MSGS;
    protected String SQL_PUT_NEW_TO_QUEUE, SQL_REPUT_TO_QUEUE;
    protected String SQL_REMOVE_FROM_EPHEMERAL;

    protected String SQL_REQUEUE, SQL_REQUEUE_SILENT;
    protected String SQL_UPDATE_EPHEMERAL_ID_TAKE, SQL_CLEAR_EPHEMERAL_ID;
    protected String SQL_READ_BY_EPHEMERAL_ID;

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdIntQueueMessage readFromQueueStorage(Connection conn) {
        // UNUSED
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdIntQueueMessage readFromEphemeralStorage(Connection conn,
            IQueueMessage<Long, byte[]> msg) {
        // UNUSED
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collection<UniversalIdIntQueueMessage> getOrphanFromEphemeralStorage(Connection conn,
            long thresholdTimestampMs) {
        Date threshold = new Date(System.currentTimeMillis() - thresholdTimestampMs);
        Collection<UniversalIdIntQueueMessage> result = new ArrayList<>();
        try (Stream<Map<String, Object>> dbRows = getJdbcHelper().executeSelectAsStream(conn,
                SQL_GET_ORPHAN_MSGS, getQueueName(), threshold)) {
            dbRows.forEach(row -> result.add(UniversalIdIntQueueMessage.newInstance(row)));
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean putToQueueStorage(Connection conn, IQueueMessage<Long, byte[]> msg) {
        Long qid = msg.qId();
        if (qid == null || qid.longValue() == 0) {
            int numRows = getJdbcHelper().execute(conn, SQL_PUT_NEW_TO_QUEUE, getQueueName(),
                    msg.qOriginalTimestamp(), msg.qTimestamp(), msg.qNumRequeues(), msg.qData());
            return numRows > 0;
        } else {
            int numRows = getJdbcHelper().execute(conn, SQL_REPUT_TO_QUEUE, getQueueName(), qid,
                    msg.qOriginalTimestamp(), msg.qTimestamp(), msg.qNumRequeues(), msg.qData());
            return numRows > 0;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean putToEphemeralStorage(Connection conn, IQueueMessage<Long, byte[]> _msg) {
        // UNUSED
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean removeFromQueueStorage(Connection conn, IQueueMessage<Long, byte[]> msg) {
        // UNUSED
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean removeFromEphemeralStorage(Connection conn, IQueueMessage<Long, byte[]> msg) {
        int numRows = getJdbcHelper().execute(conn, SQL_REMOVE_FROM_EPHEMERAL, getQueueName(),
                msg.qId());
        return numRows > 0;
    }

    /*------------------------------------------------------------*/
    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean _queueWithRetries(Connection conn, IQueueMessage<Long, byte[]> msg,
            int numRetries, int maxRetries) {
        try {
            Date now = new Date();
            msg.qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now);
            boolean result = putToQueueStorage(conn, msg);
            return result;
        } catch (DuplicatedValueException dve) {
            LOGGER.warn(dve.getMessage(), dve);
            return true;
        } catch (DaoException de) {
            if (de.getCause() instanceof DuplicateKeyException) {
                LOGGER.warn(de.getMessage(), de);
                return true;
            }
            if (de.getCause() instanceof ConcurrencyFailureException) {
                if (numRetries > maxRetries) {
                    throw new QueueException(de);
                } else {
                    return _queueWithRetries(conn, msg, numRetries + 1, maxRetries);
                }
            }
            throw de;
        } catch (Exception e) {
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    protected boolean _requeueWithRetries(Connection conn, IQueueMessage<Long, byte[]> msg,
            int numRetries, int maxRetries) {
        try {
            int numRows = getJdbcHelper().execute(conn, SQL_REQUEUE, new Date(), getQueueName(),
                    msg.qId());
            return numRows > 0;
        } catch (DuplicatedValueException dve) {
            LOGGER.warn(dve.getMessage(), dve);
            return true;
        } catch (DaoException de) {
            if (de.getCause() instanceof DuplicateKeyException) {
                LOGGER.warn(de.getMessage(), de);
                return true;
            }
            if (de.getCause() instanceof ConcurrencyFailureException) {
                if (numRetries > maxRetries) {
                    throw new QueueException(de);
                } else {
                    /*
                     * call _requeueSilentWithRetries(...) here is correct
                     * because we do not want message's num-requeues is
                     * increased with every retry
                     */
                    return _requeueSilentWithRetries(conn, msg, numRetries + 1, maxRetries);
                }
            }
            throw de;
        } catch (Exception e) {
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    protected boolean _requeueSilentWithRetries(Connection conn, IQueueMessage<Long, byte[]> msg,
            int numRetries, int maxRetries) {
        try {
            int numRows = getJdbcHelper().execute(conn, SQL_REQUEUE_SILENT, getQueueName(),
                    msg.qId());
            return numRows > 0;
        } catch (DuplicatedValueException dve) {
            LOGGER.warn(dve.getMessage(), dve);
            return true;
        } catch (DaoException de) {
            if (de.getCause() instanceof DuplicateKeyException) {
                LOGGER.warn(de.getMessage(), de);
                return true;
            }
            if (de.getCause() instanceof ConcurrencyFailureException) {
                if (numRetries > maxRetries) {
                    throw new QueueException(de);
                } else {
                    return _requeueSilentWithRetries(conn, msg, numRetries + 1, maxRetries);
                }
            }
            throw de;
        } catch (Exception e) {
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void _finishWithRetries(Connection conn, IQueueMessage<Long, byte[]> msg,
            int numRetries, int maxRetries) {
        try {
            removeFromEphemeralStorage(conn, msg);
        } catch (DaoException de) {
            if (de.getCause() instanceof ConcurrencyFailureException) {
                if (numRetries > maxRetries) {
                    throw new QueueException(de);
                } else {
                    _finishWithRetries(conn, msg, numRetries + 1, maxRetries);
                }
            }
            throw de;
        } catch (Exception e) {
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    protected UniversalIdIntQueueMessage _takeWithRetries(Connection conn, int numRetries,
            int maxRetries) {
        try {
            UniversalIdIntQueueMessage msg = null;
            long ephemeralId = QueueUtils.IDGEN.generateId64();
            int numRows = getJdbcHelper().execute(conn, SQL_UPDATE_EPHEMERAL_ID_TAKE, ephemeralId,
                    getQueueName());
            if (numRows > 0) {
                Map<String, Object> dbRow = getJdbcHelper().executeSelectOne(conn,
                        SQL_READ_BY_EPHEMERAL_ID, getQueueName(), ephemeralId);
                if (dbRow != null) {
                    msg = UniversalIdIntQueueMessage.newInstance(dbRow);
                }
            }
            return msg;
        } catch (DaoException de) {
            if (de.getCause() instanceof ConcurrencyFailureException) {
                if (numRetries > maxRetries) {
                    throw new QueueException(de);
                } else {
                    return _takeWithRetries(conn, numRetries + 1, maxRetries);
                }
            }
            throw de;
        } catch (Exception e) {
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean _moveFromEphemeralToQueueStorageWithRetries(IQueueMessage<Long, byte[]> msg,
            Connection conn, int numRetries, int maxRetries) {
        try {
            int numRows = getJdbcHelper().execute(conn, SQL_CLEAR_EPHEMERAL_ID, getQueueName(),
                    msg.qId());
            return numRows > 0;
        } catch (DaoException de) {
            if (de.getCause() instanceof ConcurrencyFailureException) {
                if (numRetries > maxRetries) {
                    throw new QueueException(de);
                } else {
                    return _moveFromEphemeralToQueueStorageWithRetries(msg, conn, numRetries + 1,
                            maxRetries);
                }
            }
            throw de;
        } catch (Exception e) {
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

}

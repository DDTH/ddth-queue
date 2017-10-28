package com.github.ddth.queue.impl.universal;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DuplicateKeyException;

import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DuplicatedValueException;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.JdbcQueue;
import com.github.ddth.queue.utils.QueueException;
import com.github.ddth.queue.utils.QueueUtils;

/**
 * Same as {@link UniversalJdbcQueue}, but using a less-locking algorithm,
 * requires only one single db table for both queue and ephemeral storages.
 * 
 * <p>
 * Queue db table schema:
 * </p>
 * <ul>
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
 * @since 0.5.1.1
 */
public class AbstractLessLockingUniversalJdbcQueue extends JdbcQueue {

    private Logger LOGGER = LoggerFactory.getLogger(AbstractLessLockingUniversalJdbcQueue.class);

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
    public AbstractLessLockingUniversalJdbcQueue setFifo(boolean fifo) {
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
    public AbstractLessLockingUniversalJdbcQueue markFifo(boolean fifo) {
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
    protected UniversalQueueMessage readFromQueueStorage(Connection conn) {
        // UNUSED
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalQueueMessage readFromEphemeralStorage(Connection conn, IQueueMessage msg) {
        // UNUSED
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collection<IQueueMessage> getOrphanFromEphemeralStorage(Connection conn,
            long thresholdTimestampMs) {
        final Date threshold = new Date(System.currentTimeMillis() - thresholdTimestampMs);
        List<Map<String, Object>> dbRows = getJdbcHelper().executeSelect(conn, SQL_GET_ORPHAN_MSGS,
                threshold);
        if (dbRows != null && dbRows.size() > 0) {
            Collection<IQueueMessage> result = new ArrayList<IQueueMessage>();
            for (Map<String, Object> dbRow : dbRows) {
                UniversalQueueMessage msg = new UniversalQueueMessage();
                // ensureContentIsByteArray(dbRow);
                msg.fromMap(dbRow);
                result.add(msg);
            }
            return result;
        }
        return Arrays.asList();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean putToQueueStorage(Connection conn, IQueueMessage _msg) {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }
        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        Long qid = msg.qId();
        if (qid == null || qid.longValue() == 0) {
            int numRows = getJdbcHelper().execute(conn, SQL_PUT_NEW_TO_QUEUE,
                    msg.qOriginalTimestamp(), msg.qTimestamp(), msg.qNumRequeues(), msg.content());
            return numRows > 0;
        } else {
            int numRows = getJdbcHelper().execute(conn, SQL_REPUT_TO_QUEUE, qid,
                    msg.qOriginalTimestamp(), msg.qTimestamp(), msg.qNumRequeues(), msg.content());
            return numRows > 0;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean putToEphemeralStorage(Connection conn, IQueueMessage _msg) {
        // UNUSED
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean removeFromQueueStorage(Connection conn, IQueueMessage msg) {
        // UNUSED
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean removeFromEphemeralStorage(Connection conn, IQueueMessage _msg) {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }
        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        int numRows = getJdbcHelper().execute(conn, SQL_REMOVE_FROM_EPHEMERAL, msg.qId());
        return numRows > 0;
    }

    /*------------------------------------------------------------*/
    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean _queueWithRetries(Connection conn, IQueueMessage _msg, int numRetries,
            int maxRetries) {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }
        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
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
    protected boolean _requeueWithRetries(Connection conn, IQueueMessage _msg, int numRetries,
            int maxRetries) {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }
        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        try {
            int numRows = getJdbcHelper().execute(conn, SQL_REQUEUE, new Date(), msg.qId());
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
    protected boolean _requeueSilentWithRetries(Connection conn, IQueueMessage _msg, int numRetries,
            int maxRetries) {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }
        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        try {
            int numRows = getJdbcHelper().execute(conn, SQL_REQUEUE_SILENT, msg.qId());
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
    protected void _finishWithRetries(Connection conn, IQueueMessage _msg, int numRetries,
            int maxRetries) {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }
        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
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
    protected UniversalQueueMessage _takeWithRetries(Connection conn, int numRetries,
            int maxRetries) {
        try {
            UniversalQueueMessage msg = null;
            long ephemeralId = QueueUtils.IDGEN.generateId64();
            int numRows = getJdbcHelper().execute(conn, SQL_UPDATE_EPHEMERAL_ID_TAKE, ephemeralId);
            if (numRows > 0) {
                List<Map<String, Object>> dbRows = getJdbcHelper().executeSelect(conn,
                        SQL_READ_BY_EPHEMERAL_ID, ephemeralId);
                if (dbRows != null && dbRows.size() > 0) {
                    Map<String, Object> dbRow = dbRows.get(0);
                    // ensureContentIsByteArray(dbRow);
                    msg = new UniversalQueueMessage();
                    msg.fromMap(dbRow);
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
    protected boolean _moveFromEphemeralToQueueStorageWithRetries(IQueueMessage _msg,
            Connection conn, int numRetries, int maxRetries) {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }
        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        try {
            int numRows = getJdbcHelper().execute(conn, SQL_CLEAR_EPHEMERAL_ID, msg.qId());
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

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalQueueMessage take() {
        return (UniversalQueueMessage) super.take();
    }
}

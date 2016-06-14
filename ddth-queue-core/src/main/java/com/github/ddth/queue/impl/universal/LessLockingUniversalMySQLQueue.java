package com.github.ddth.queue.impl.universal;

import java.sql.Connection;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.dao.PessimisticLockingFailureException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.JdbcQueue;
import com.github.ddth.queue.utils.QueueException;
import com.github.ddth.queue.utils.QueueUtils;

/**
 * Same as {@link UniversalJdbcQueue}, but using a less-locking algorithm -
 * specific for MySQL, and requires only one single db table for both queue and
 * ephemeral storages.
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
 * @since 0.2.3
 */
public class LessLockingUniversalMySQLQueue extends JdbcQueue {

    private Logger LOGGER = LoggerFactory.getLogger(LessLockingUniversalMySQLQueue.class);

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
    public LessLockingUniversalMySQLQueue setFifo(boolean fifo) {
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
    public LessLockingUniversalMySQLQueue markFifo(boolean fifo) {
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

    private String SQL_GET_ORPHAN_MSGS;
    private String SQL_PUT_NEW_TO_QUEUE, SQL_REPUT_TO_QUEUE;
    private String SQL_REMOVE_FROM_EPHEMERAL;

    private String SQL_REQUEUE, SQL_REQUEUE_SILENT;
    private String SQL_UPDATE_EPHEMERAL_ID_TAKE, SQL_CLEAR_EPHEMERAL_ID;
    private String SQL_READ_BY_EPHEMERAL_ID;

    public LessLockingUniversalMySQLQueue init() {
        super.init();

        /*
         * Sets column COL_EPHEMERAL_ID's value to nil, increases value of
         * column COL_NUM_REQUEUES, also updates value of column COL_TIMESTAMP
         */
        SQL_REQUEUE = "UPDATE {0} SET {1}=0, {2}={2}+1, {3}=? WHERE {4}=?";
        SQL_REQUEUE = MessageFormat.format(SQL_REQUEUE, getTableName(), COL_EPHEMERAL_ID,
                COL_NUM_REQUEUES, COL_TIMESTAMP, COL_QUEUE_ID);

        /*
         * Sets column COL_EPHEMERAL_ID's value to nil
         */
        SQL_REQUEUE_SILENT = "UPDATE {0} SET {1}=0 WHERE {2}=?";
        SQL_REQUEUE_SILENT = MessageFormat.format(SQL_REQUEUE_SILENT, getTableName(),
                COL_EPHEMERAL_ID, COL_QUEUE_ID);

        /*
         * Sets value of column COL_EPHEMERAL_ID for taking a queue message
         */
        SQL_UPDATE_EPHEMERAL_ID_TAKE = "UPDATE {0} SET {1}=? WHERE {1}=0"
                + (fifo ? (" ORDER BY " + COL_QUEUE_ID) : "") + " LIMIT 1";
        SQL_UPDATE_EPHEMERAL_ID_TAKE = MessageFormat.format(SQL_UPDATE_EPHEMERAL_ID_TAKE,
                getTableName(), COL_EPHEMERAL_ID);

        /*
         * Sets column COL_EPHEMERAL_ID's value to nil (used in action
         * "move message from ephemeral storage to queue")
         */
        SQL_CLEAR_EPHEMERAL_ID = "UPDATE {0} SET {1}=0 WHERE {2}=?";
        SQL_CLEAR_EPHEMERAL_ID = MessageFormat.format(SQL_CLEAR_EPHEMERAL_ID, getTableName(),
                COL_EPHEMERAL_ID, COL_QUEUE_ID);

        /*
         * Reads a queue message by ephemeral id
         */
        SQL_READ_BY_EPHEMERAL_ID = "SELECT {1}, {2}, {3}, {4}, {5} FROM {0} WHERE {6}=?";
        SQL_READ_BY_EPHEMERAL_ID = MessageFormat.format(SQL_READ_BY_EPHEMERAL_ID, getTableName(),
                COL_QUEUE_ID + " AS " + UniversalQueueMessage.FIELD_QUEUE_ID,
                COL_ORG_TIMESTAMP + " AS " + UniversalQueueMessage.FIELD_ORG_TIMESTAMP,
                COL_TIMESTAMP + " AS " + UniversalQueueMessage.FIELD_TIMESTAMP,
                COL_NUM_REQUEUES + " AS " + UniversalQueueMessage.FIELD_NUM_REQUEUES,
                COL_CONTENT + " AS " + UniversalQueueMessage.FIELD_CONTENT, COL_EPHEMERAL_ID);

        SQL_GET_ORPHAN_MSGS = "SELECT {1}, {2}, {3}, {4}, {5} FROM {0} WHERE " + COL_EPHEMERAL_ID
                + "!=0 AND " + COL_TIMESTAMP + "<?";
        SQL_GET_ORPHAN_MSGS = MessageFormat.format(SQL_GET_ORPHAN_MSGS, getTableNameEphemeral(),
                COL_QUEUE_ID + " AS " + UniversalQueueMessage.FIELD_QUEUE_ID,
                COL_ORG_TIMESTAMP + " AS " + UniversalQueueMessage.FIELD_ORG_TIMESTAMP,
                COL_TIMESTAMP + " AS " + UniversalQueueMessage.FIELD_TIMESTAMP,
                COL_NUM_REQUEUES + " AS " + UniversalQueueMessage.FIELD_NUM_REQUEUES,
                COL_CONTENT + " AS " + UniversalQueueMessage.FIELD_CONTENT);

        /*
         * Puts a new message (message without pre-set queue id) to queue,
         * assuming column COL_QUEUE_ID is auto-number
         */
        SQL_PUT_NEW_TO_QUEUE = "INSERT INTO {0} ({1}, {2}, {3}, {4}) VALUES (?, ?, ?, ?)";
        SQL_PUT_NEW_TO_QUEUE = MessageFormat.format(SQL_PUT_NEW_TO_QUEUE, getTableName(),
                COL_ORG_TIMESTAMP, COL_TIMESTAMP, COL_NUM_REQUEUES, COL_CONTENT);

        /*
         * Put a message with pre-set queue id to queue
         */
        SQL_REPUT_TO_QUEUE = "INSERT INTO {0} ({1}, {2}, {3}, {4}, {5}) VALUES (?, ?, ?, ?, ?)";
        SQL_REPUT_TO_QUEUE = MessageFormat.format(SQL_REPUT_TO_QUEUE, getTableName(), COL_QUEUE_ID,
                COL_ORG_TIMESTAMP, COL_TIMESTAMP, COL_NUM_REQUEUES, COL_CONTENT);

        SQL_REMOVE_FROM_EPHEMERAL = "DELETE FROM {0} WHERE " + COL_QUEUE_ID + "=?";
        SQL_REMOVE_FROM_EPHEMERAL = MessageFormat.format(SQL_REMOVE_FROM_EPHEMERAL,
                getTableNameEphemeral());

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalQueueMessage readFromQueueStorage(JdbcTemplate jdbcTemplate) {
        // UNUSED
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalQueueMessage readFromEphemeralStorage(JdbcTemplate jdbcTemplate,
            IQueueMessage msg) {
        // UNUSED
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collection<IQueueMessage> getOrphanFromEphemeralStorage(JdbcTemplate jdbcTemplate,
            long thresholdTimestampMs) {
        final Date threshold = new Date(thresholdTimestampMs);
        List<Map<String, Object>> dbRows = jdbcTemplate.queryForList(SQL_GET_ORPHAN_MSGS,
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
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean putToQueueStorage(JdbcTemplate jdbcTemplate, IQueueMessage _msg) {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }

        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        Long qid = msg.qId();
        if (qid == null || qid.longValue() == 0) {
            int numRows = jdbcTemplate.update(SQL_PUT_NEW_TO_QUEUE, msg.qOriginalTimestamp(),
                    msg.qTimestamp(), msg.qNumRequeues(), msg.content());
            return numRows > 0;
        } else {
            int numRows = jdbcTemplate.update(SQL_REPUT_TO_QUEUE, qid, msg.qOriginalTimestamp(),
                    msg.qTimestamp(), msg.qNumRequeues(), msg.content());
            return numRows > 0;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean putToEphemeralStorage(JdbcTemplate jdbcTemplate, IQueueMessage _msg) {
        // UNUSED
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean removeFromQueueStorage(JdbcTemplate jdbcTemplate, IQueueMessage msg) {
        // UNUSED
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean removeFromEphemeralStorage(JdbcTemplate jdbcTemplate, IQueueMessage _msg) {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }

        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        int numRows = jdbcTemplate.update(SQL_REMOVE_FROM_EPHEMERAL, msg.qId());
        return numRows > 0;
    }

    /*------------------------------------------------------------*/
    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean _queueWithRetries(final Connection conn, final IQueueMessage _msg,
            final int numRetries, final int maxRetries) throws SQLException {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }

        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        try {
            JdbcTemplate jdbcTemplate = jdbcTemplate(conn);
            Date now = new Date();
            msg.qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now);
            boolean result = putToQueueStorage(jdbcTemplate, msg);
            return result;
        } catch (DuplicateKeyException dke) {
            LOGGER.warn(dke.getMessage(), dke);
            return true;
        } catch (PessimisticLockingFailureException ex) {
            if (numRetries > maxRetries) {
                throw new QueueException(ex);
            } else {
                return _queueWithRetries(conn, msg, numRetries + 1, maxRetries);
            }
        } catch (Exception e) {
            throw new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    protected boolean _requeueWithRetries(final Connection conn, final IQueueMessage _msg,
            final int numRetries, final int maxRetries) throws SQLException {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }

        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        try {
            JdbcTemplate jdbcTemplate = jdbcTemplate(conn);
            int numRows = jdbcTemplate.update(SQL_REQUEUE, new Date(), msg.qId());
            return numRows > 0;
        } catch (DuplicateKeyException dke) {
            LOGGER.warn(dke.getMessage(), dke);
            return true;
        } catch (PessimisticLockingFailureException ex) {
            if (numRetries > maxRetries) {
                throw new QueueException(ex);
            } else {
                /*
                 * call _requeueSilentWithRetries(...) here is correct because
                 * we do not want message's num-requeues is increased with every
                 * retry
                 */
                return _requeueSilentWithRetries(conn, msg, numRetries + 1, maxRetries);
            }
        } catch (Exception e) {
            throw new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    protected boolean _requeueSilentWithRetries(final Connection conn, final IQueueMessage _msg,
            final int numRetries, final int maxRetries) throws SQLException {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }

        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        try {
            JdbcTemplate jdbcTemplate = jdbcTemplate(conn);
            int numRows = jdbcTemplate.update(SQL_REQUEUE_SILENT, msg.qId());
            return numRows > 0;
        } catch (DuplicateKeyException dke) {
            LOGGER.warn(dke.getMessage(), dke);
            return true;
        } catch (PessimisticLockingFailureException ex) {
            if (numRetries > maxRetries) {
                throw new QueueException(ex);
            } else {
                return _requeueSilentWithRetries(conn, msg, numRetries + 1, maxRetries);
            }
        } catch (Exception e) {
            throw new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void _finishWithRetries(final Connection conn, final IQueueMessage _msg,
            final int numRetries, final int maxRetries) throws SQLException {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }

        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        try {
            JdbcTemplate jdbcTemplate = jdbcTemplate(conn);
            removeFromEphemeralStorage(jdbcTemplate, msg);
        } catch (PessimisticLockingFailureException ex) {
            if (numRetries > maxRetries) {
                throw new QueueException(ex);
            } else {
                _finishWithRetries(conn, msg, numRetries + 1, maxRetries);
            }
        } catch (Exception e) {
            throw new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    protected UniversalQueueMessage _takeWithRetries(final Connection conn, final int numRetries,
            final int maxRetries) throws SQLException {
        try {
            JdbcTemplate jdbcTemplate = jdbcTemplate(conn);

            UniversalQueueMessage msg = null;
            long ephemeralId = QueueUtils.IDGEN.generateId64();
            int numRows = jdbcTemplate.update(SQL_UPDATE_EPHEMERAL_ID_TAKE, ephemeralId);
            if (numRows > 0) {
                List<Map<String, Object>> dbRows = jdbcTemplate
                        .queryForList(SQL_READ_BY_EPHEMERAL_ID, ephemeralId);
                if (dbRows != null && dbRows.size() > 0) {
                    Map<String, Object> dbRow = dbRows.get(0);
                    // ensureContentIsByteArray(dbRow);
                    msg = new UniversalQueueMessage();
                    msg.fromMap(dbRow);
                }
            }

            return msg;
        } catch (PessimisticLockingFailureException ex) {
            if (numRetries > maxRetries) {
                throw new QueueException(ex);
            } else {
                return _takeWithRetries(conn, numRetries + 1, maxRetries);
            }
        } catch (Exception e) {
            throw new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean _moveFromEphemeralToQueueStorageWithRetries(final IQueueMessage _msg,
            final Connection conn, final int numRetries, final int maxRetries) throws SQLException {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }

        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        try {
            JdbcTemplate jdbcTemplate = jdbcTemplate(conn);
            int numRows = jdbcTemplate.update(SQL_CLEAR_EPHEMERAL_ID, msg.qId());
            return numRows > 0;
        } catch (PessimisticLockingFailureException ex) {
            if (numRetries > maxRetries) {
                throw new QueueException(ex);
            } else {
                return _moveFromEphemeralToQueueStorageWithRetries(msg, conn, numRetries + 1,
                        maxRetries);
            }
        } catch (Exception e) {
            throw new QueueException(e);
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

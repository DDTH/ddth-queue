package com.github.ddth.queue.impl;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DuplicateKeyException;

import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DuplicatedValueException;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;

/**
 * Abstract JDBC implementation of {@link IQueue}.
 * 
 * <p>
 * Implementation:
 * <ul>
 * <li>Queue storage & Ephemeral storage are 2 database tables, same structure!
 * </li>
 * </ul>
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public abstract class JdbcQueue<ID, DATA> extends AbstractEphemeralSupportQueue<ID, DATA> {

    public static int DEFAULT_MAX_RETRIES = 3;

    private Logger LOGGER = LoggerFactory.getLogger(JdbcQueue.class);

    private final static String FIELD_COUNT = "num_entries";
    private String tableName, tableNameEphemeral;
    private String SQL_COUNT = "SELECT COUNT(*) AS " + FIELD_COUNT + " FROM {0}";
    private String SQL_COUNT_EPHEMERAL = "SELECT COUNT(*) AS " + FIELD_COUNT + " FROM {0}";

    private IJdbcHelper jdbcHelper;
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private int transactionIsolationLevel = Connection.TRANSACTION_READ_COMMITTED;

    /*----------------------------------------------------------------------*/
    public JdbcQueue<ID, DATA> setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public JdbcQueue<ID, DATA> setTableNameEphemeral(String tableNameEphemeral) {
        this.tableNameEphemeral = tableNameEphemeral;
        return this;
    }

    public String getTableNameEphemeral() {
        return tableNameEphemeral;
    }

    public JdbcQueue<ID, DATA> setTransactionIsolationLevel(int transactionIsolationLevel) {
        this.transactionIsolationLevel = transactionIsolationLevel;
        return this;
    }

    public int getTransactionIsolationLevel() {
        return transactionIsolationLevel;
    }

    /**
     * 
     * @return
     * @since 0.5.0
     */
    public IJdbcHelper getJdbcHelper() {
        return jdbcHelper;
    }

    /**
     * 
     * @param jdbcHelper
     * @return
     * @since 0.5.1.1
     */
    public JdbcQueue<ID, DATA> setJdbcHelper(IJdbcHelper jdbcHelper) {
        this.jdbcHelper = jdbcHelper;
        return this;
    }

    public JdbcQueue<ID, DATA> setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcQueue<ID, DATA> init() {
        SQL_COUNT = MessageFormat.format(SQL_COUNT, getTableName());
        SQL_COUNT_EPHEMERAL = MessageFormat.format(SQL_COUNT_EPHEMERAL, getTableNameEphemeral());
        return this;
    }

    /*----------------------------------------------------------------------*/

    /**
     * Read a message from head of queue storage.
     * 
     * @param conn
     * @return
     */
    protected abstract IQueueMessage<ID, DATA> readFromQueueStorage(Connection conn);

    /**
     * Read a message from the ephemeral storage.
     * 
     * @param conn
     * @param msg
     * @return
     * @since 0.2.1
     */
    protected abstract IQueueMessage<ID, DATA> readFromEphemeralStorage(Connection conn,
            IQueueMessage<ID, DATA> msg);

    /**
     * Get all orphan messages (messages that were left in ephemeral storage for
     * a long time).
     * 
     * @param conn
     * @param thresholdTimestampMs
     *            get all orphan messages that were queued
     *            <strong>before</strong> this timestamp
     * @return
     * @since 0.2.0
     */
    protected abstract Collection<? extends IQueueMessage<ID, DATA>> getOrphanFromEphemeralStorage(
            Connection conn, long thresholdTimestampMs);

    /**
     * Put a message to tail of the queue storage.
     * 
     * @param conn
     * @param msg
     * @return
     */
    protected abstract boolean putToQueueStorage(Connection conn, IQueueMessage<ID, DATA> msg);

    /**
     * Put a message to the ephemeral storage.
     * 
     * @param conn
     * @param msg
     * @return
     */
    protected abstract boolean putToEphemeralStorage(Connection conn, IQueueMessage<ID, DATA> msg);

    /**
     * Remove a message from the queue storage.
     * 
     * @param conn
     * @param msg
     * @return
     */
    protected abstract boolean removeFromQueueStorage(Connection conn, IQueueMessage<ID, DATA> msg);

    /**
     * Remove a message from the queue storage.
     * 
     * @param conn
     * @param msg
     * @return
     */
    protected abstract boolean removeFromEphemeralStorage(Connection conn,
            IQueueMessage<ID, DATA> msg);

    /**
     * Queue a message, retry if deadlock.
     * 
     * <p>
     * Note: http://dev.mysql.com/doc/refman/5.0/en/innodb-deadlocks.html
     * </p>
     * <p>
     * InnoDB uses automatic row-level locking. You can get deadlocks even in
     * the case of transactions that just insert or delete a single row. That is
     * because these operations are not really "atomic"; they automatically set
     * locks on the (possibly several) index records of the row inserted or
     * deleted.
     * </p>
     * 
     * <p>
     * Note: the supplied queue message is mutable.
     * </p>
     * 
     * @param conn
     * @param msg
     * @param numRetries
     * @param maxRetries
     * @return
     */
    protected boolean _queueWithRetries(Connection conn, IQueueMessage<ID, DATA> msg,
            int numRetries, int maxRetries) {
        try {
            Date now = new Date();
            msg.qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now);
            return putToQueueStorage(conn, msg);
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
    @Override
    public boolean queue(IQueueMessage<ID, DATA> msg) {
        if (msg == null) {
            return false;
        }
        try {
            try (Connection conn = jdbcHelper.getConnection()) {
                return _queueWithRetries(conn, msg.clone(), 0, this.maxRetries);
            }
        } catch (Exception e) {
            final String logMsg = "(queue) Exception [" + e.getClass().getName() + "]: "
                    + e.getMessage();
            LOGGER.error(logMsg, e);
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * Re-queue a message, retry if deadlock.
     * 
     * <p>
     * Note: http://dev.mysql.com/doc/refman/5.0/en/innodb-deadlocks.html
     * </p>
     * <p>
     * InnoDB uses automatic row-level locking. You can get deadlocks even in
     * the case of transactions that just insert or delete a single row. That is
     * because these operations are not really "atomic"; they automatically set
     * locks on the (possibly several) index records of the row inserted or
     * deleted.
     * </p>
     * 
     * <p>
     * Note: the supplied queue message is mutable.
     * </p>
     * 
     * @param conn
     * @param msg
     * @param numRetries
     * @param maxRetries
     * @return
     */
    protected boolean _requeueWithRetries(Connection conn, IQueueMessage<ID, DATA> msg,
            int numRetries, int maxRetries) {
        try {
            jdbcHelper.startTransaction(conn);
            conn.setTransactionIsolation(transactionIsolationLevel);
            if (!isEphemeralDisabled()) {
                removeFromEphemeralStorage(conn, msg);
            }
            Date now = new Date();
            msg.qIncNumRequeues().qTimestamp(now);
            boolean result = putToQueueStorage(conn, msg);
            jdbcHelper.commitTransaction(conn);
            return result;
        } catch (DuplicatedValueException dve) {
            jdbcHelper.rollbackTransaction(conn);
            LOGGER.warn(dve.getMessage(), dve);
            return true;
        } catch (DaoException de) {
            if (de.getCause() instanceof DuplicateKeyException) {
                jdbcHelper.rollbackTransaction(conn);
                LOGGER.warn(de.getMessage(), de);
                return true;
            }
            if (de.getCause() instanceof ConcurrencyFailureException) {
                jdbcHelper.rollbackTransaction(conn);
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
            jdbcHelper.rollbackTransaction(conn);
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeue(IQueueMessage<ID, DATA> msg) {
        if (msg == null) {
            return false;
        }
        try {
            try (Connection conn = jdbcHelper.getConnection()) {
                return _requeueWithRetries(conn, msg.clone(), 0, this.maxRetries);
            }
        } catch (Exception e) {
            final String logMsg = "(requeue) Exception [" + e.getClass().getName() + "]: "
                    + e.getMessage();
            LOGGER.error(logMsg, e);
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * Re-queue a message silently, retry if deadlock.
     * 
     * <p>
     * Note: http://dev.mysql.com/doc/refman/5.0/en/innodb-deadlocks.html
     * </p>
     * <p>
     * InnoDB uses automatic row-level locking. You can get deadlocks even in
     * the case of transactions that just insert or delete a single row. That is
     * because these operations are not really "atomic"; they automatically set
     * locks on the (possibly several) index records of the row inserted or
     * deleted.
     * </p>
     * 
     * <p>
     * Note: the supplied queue message is mutable.
     * </p>
     * 
     * @param conn
     * @param msg
     * @param numRetries
     * @param maxRetries
     * @return
     */
    protected boolean _requeueSilentWithRetries(Connection conn, IQueueMessage<ID, DATA> msg,
            int numRetries, int maxRetries) {
        try {
            jdbcHelper.startTransaction(conn);
            conn.setTransactionIsolation(transactionIsolationLevel);
            if (!isEphemeralDisabled()) {
                removeFromEphemeralStorage(conn, msg);
            }
            boolean result = putToQueueStorage(conn, msg);
            jdbcHelper.commitTransaction(conn);
            return result;
        } catch (DuplicatedValueException dve) {
            jdbcHelper.rollbackTransaction(conn);
            LOGGER.warn(dve.getMessage(), dve);
            return true;
        } catch (DaoException de) {
            if (de.getCause() instanceof DuplicateKeyException) {
                jdbcHelper.rollbackTransaction(conn);
                LOGGER.warn(de.getMessage(), de);
                return true;
            }
            if (de.getCause() instanceof ConcurrencyFailureException) {
                jdbcHelper.rollbackTransaction(conn);
                if (numRetries > maxRetries) {
                    throw new QueueException(de);
                } else {
                    return _requeueSilentWithRetries(conn, msg, numRetries + 1, maxRetries);
                }
            }
            throw de;
        } catch (Exception e) {
            jdbcHelper.rollbackTransaction(conn);
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeueSilent(IQueueMessage<ID, DATA> msg) {
        if (msg == null) {
            return false;
        }
        try {
            try (Connection conn = jdbcHelper.getConnection()) {
                return _requeueSilentWithRetries(conn, msg.clone(), 0, this.maxRetries);
            }
        } catch (Exception e) {
            final String logMsg = "(requeueSilent) Exception [" + e.getClass().getName() + "]: "
                    + e.getMessage();
            LOGGER.error(logMsg, e);
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * Perform "finish" action, retry if deadlock.
     * 
     * <p>
     * Note: http://dev.mysql.com/doc/refman/5.0/en/innodb-deadlocks.html
     * </p>
     * <p>
     * InnoDB uses automatic row-level locking. You can get deadlocks even in
     * the case of transactions that just insert or delete a single row. That is
     * because these operations are not really "atomic"; they automatically set
     * locks on the (possibly several) index records of the row inserted or
     * deleted.
     * </p>
     * 
     * <p>
     * Note: the supplied queue message is mutable.
     * </p>
     * 
     * @param conn
     * @param msg
     * @param numRetries
     * @param maxRetries
     */
    protected void _finishWithRetries(Connection conn, IQueueMessage<ID, DATA> msg, int numRetries,
            int maxRetries) {
        try {
            if (!isEphemeralDisabled()) {
                removeFromEphemeralStorage(conn, msg);
            }
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
    @Override
    public void finish(IQueueMessage<ID, DATA> msg) {
        if (msg == null) {
            return;
        }
        try {
            try (Connection conn = jdbcHelper.getConnection()) {
                _finishWithRetries(conn, msg, 0, this.maxRetries);
            }
        } catch (Exception e) {
            final String logMsg = "(finish) Exception [" + e.getClass().getName() + "]: "
                    + e.getMessage();
            LOGGER.error(logMsg, e);
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * Take a message from queue, retry if deadlock.
     * 
     * <p>
     * Note: http://dev.mysql.com/doc/refman/5.0/en/innodb-deadlocks.html
     * </p>
     * <p>
     * InnoDB uses automatic row-level locking. You can get deadlocks even in
     * the case of transactions that just insert or delete a single row. That is
     * because these operations are not really "atomic"; they automatically set
     * locks on the (possibly several) index records of the row inserted or
     * deleted.
     * </p>
     * 
     * @param conn
     * @param numRetries
     * @param maxRetries
     * @return
     */
    protected IQueueMessage<ID, DATA> _takeWithRetries(Connection conn, int numRetries,
            int maxRetries) {
        try {
            jdbcHelper.startTransaction(conn);
            conn.setTransactionIsolation(transactionIsolationLevel);

            boolean result = true;
            IQueueMessage<ID, DATA> msg = readFromQueueStorage(conn);
            if (msg != null) {
                result = result && removeFromQueueStorage(conn, msg);
                if (!isEphemeralDisabled()) {
                    try {
                        result = result && putToEphemeralStorage(conn, msg);
                    } catch (DuplicatedValueException dve) {
                        LOGGER.warn(dve.getMessage(), dve);
                    } catch (DaoException de) {
                        if (de.getCause() instanceof DuplicatedValueException) {
                            LOGGER.warn(de.getMessage(), de);
                        } else {
                            throw de;
                        }
                    }
                }
            }
            if (result) {
                jdbcHelper.commitTransaction(conn);
                return msg;
            } else {
                jdbcHelper.rollbackTransaction(conn);
                return null;
            }
        } catch (DaoException de) {
            if (de.getCause() instanceof ConcurrencyFailureException) {
                jdbcHelper.rollbackTransaction(conn);
                if (numRetries > maxRetries) {
                    throw new QueueException(de);
                } else {
                    return _takeWithRetries(conn, numRetries + 1, maxRetries);
                }
            }
            throw de;
        } catch (Exception e) {
            jdbcHelper.rollbackTransaction(conn);
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @throws QueueException.EphemeralIsFull
     *             if the ephemeral storage is full
     */
    @Override
    public IQueueMessage<ID, DATA> take() throws QueueException.EphemeralIsFull {
        try {
            try (Connection conn = jdbcHelper.getConnection()) {
                if (!isEphemeralDisabled()) {
                    int ephemeralMaxSize = getEphemeralMaxSize();
                    if (ephemeralMaxSize > 0 && ephemeralSize(conn) >= ephemeralMaxSize) {
                        throw new QueueException.EphemeralIsFull(ephemeralMaxSize);
                    }
                }
                return _takeWithRetries(conn, 0, this.maxRetries);
            }
        } catch (Exception e) {
            final String logMsg = "(take) Exception [" + e.getClass().getName() + "]: "
                    + e.getMessage();
            LOGGER.error(logMsg, e);
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * Get all orphan messages (messages that were left in ephemeral storage for
     * a long time), retry if deadlock.
     * 
     * <p>
     * Note: http://dev.mysql.com/doc/refman/5.0/en/innodb-deadlocks.html
     * </p>
     * <p>
     * InnoDB uses automatic row-level locking. You can get deadlocks even in
     * the case of transactions that just insert or delete a single row. That is
     * because these operations are not really "atomic"; they automatically set
     * locks on the (possibly several) index records of the row inserted or
     * deleted.
     * </p>
     * 
     * @param thresholdTimestampMs
     * @param conn
     * @param numRetries
     * @param maxRetries
     * @return
     * @since 0.2.0
     */
    protected Collection<? extends IQueueMessage<ID, DATA>> _getOrphanMessagesWithRetries(
            long thresholdTimestampMs, Connection conn, int numRetries, int maxRetries) {
        try {
            jdbcHelper.startTransaction(conn);
            conn.setTransactionIsolation(transactionIsolationLevel);
            Collection<? extends IQueueMessage<ID, DATA>> msgs = getOrphanFromEphemeralStorage(conn,
                    thresholdTimestampMs);
            jdbcHelper.commitTransaction(conn);
            return msgs;
        } catch (DaoException de) {
            if (de.getCause() instanceof ConcurrencyFailureException) {
                jdbcHelper.rollbackTransaction(conn);
                if (numRetries > maxRetries) {
                    throw new QueueException(de);
                } else {
                    return _getOrphanMessagesWithRetries(thresholdTimestampMs, conn, numRetries + 1,
                            maxRetries);
                }
            }
            throw de;
        } catch (Exception e) {
            jdbcHelper.rollbackTransaction(conn);
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<IQueueMessage<ID, DATA>> getOrphanMessages(long thresholdTimestampMs) {
        if (isEphemeralDisabled()) {
            return null;
        }
        try (Connection conn = jdbcHelper.getConnection()) {
            return Collections.unmodifiableCollection(
                    _getOrphanMessagesWithRetries(thresholdTimestampMs, conn, 0, this.maxRetries));
        } catch (Exception e) {
            final String logMsg = "(getOrphanMessages) Exception [" + e.getClass().getName() + "]: "
                    + e.getMessage();
            LOGGER.error(logMsg, e);
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * Move a message from ephemeral back to queue storage, retry if deadlock.
     * 
     * <p>
     * Note: http://dev.mysql.com/doc/refman/5.0/en/innodb-deadlocks.html
     * </p>
     * <p>
     * InnoDB uses automatic row-level locking. You can get deadlocks even in
     * the case of transactions that just insert or delete a single row. That is
     * because these operations are not really "atomic"; they automatically set
     * locks on the (possibly several) index records of the row inserted or
     * deleted.
     * </p>
     * 
     * @param msg
     * @param conn
     * @param numRetries
     * @param maxRetries
     * @return
     */
    protected boolean _moveFromEphemeralToQueueStorageWithRetries(IQueueMessage<ID, DATA> msg,
            Connection conn, int numRetries, int maxRetries) {
        try {
            jdbcHelper.startTransaction(conn);
            conn.setTransactionIsolation(transactionIsolationLevel);
            IQueueMessage<ID, DATA> orphanMsg = readFromEphemeralStorage(conn, msg);
            if (orphanMsg != null) {
                removeFromEphemeralStorage(conn, msg);
                boolean result = putToQueueStorage(conn, msg);
                jdbcHelper.commitTransaction(conn);
                return result;
            }
            jdbcHelper.rollbackTransaction(conn);
            return false;
        } catch (DaoException de) {
            if (de.getCause() instanceof ConcurrencyFailureException) {
                jdbcHelper.rollbackTransaction(conn);
                if (numRetries > maxRetries) {
                    throw new QueueException(de);
                } else {
                    return _moveFromEphemeralToQueueStorageWithRetries(msg, conn, numRetries + 1,
                            maxRetries);
                }
            }
            throw de;
        } catch (Exception e) {
            jdbcHelper.rollbackTransaction(conn);
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean moveFromEphemeralToQueueStorage(IQueueMessage<ID, DATA> msg) {
        if (isEphemeralDisabled()) {
            return true;
        }
        try (Connection conn = jdbcHelper.getConnection()) {
            return _moveFromEphemeralToQueueStorageWithRetries(msg, conn, 0, this.maxRetries);
        } catch (Exception e) {
            final String logMsg = "(moveFromEphemeralToQueueStorage) Exception ["
                    + e.getClass().getName() + "]: " + e.getMessage();
            LOGGER.error(logMsg, e);
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * Get number of items currently in queue storage.
     * 
     * @param conn
     * @return
     * @since 0.5.0
     */
    protected int queueSize(Connection conn) {
        Map<String, Object> row = jdbcHelper.executeSelectOne(conn, SQL_COUNT);
        Integer result = DPathUtils.getValue(row, FIELD_COUNT, Integer.class);
        return result != null ? result.intValue() : 0;
    }

    /**
     * Get number of items currently in ephemeral storage.
     * 
     * @param conn
     * @return
     * @since 0.5.0
     */
    protected int ephemeralSize(Connection conn) {
        Map<String, Object> row = jdbcHelper.executeSelectOne(conn, SQL_COUNT_EPHEMERAL);
        Integer result = DPathUtils.getValue(row, FIELD_COUNT, Integer.class);
        return result != null ? result.intValue() : 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        try (Connection conn = jdbcHelper.getConnection()) {
            return queueSize(conn);
        } catch (Exception e) {
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        if (isEphemeralDisabled()) {
            return 0;
        }
        try (Connection conn = jdbcHelper.getConnection()) {
            return ephemeralSize(conn);
        } catch (Exception e) {
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }
}

package com.github.ddth.queue.impl;

import com.github.ddth.commons.utils.DPathUtils;
import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;
import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.dao.utils.DuplicatedValueException;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.ConcurrencyFailureException;
import org.springframework.dao.DuplicateKeyException;

import javax.sql.DataSource;
import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Abstract JDBC implementation of {@link IQueue}.
 *
 * <p>Features:</p>
 * <ul>
 * <li>Queue-size support: yes</li>
 * <li>Ephemeral storage support: yes</li>
 * <li>Ephemeral-size support: yes</li>
 * </ul>
 *
 * <p>
 * Implementation:
 * <ul>
 * <li>Queue storage & Ephemeral storage are 2 database tables, same structure!</li>
 * <li>{@link #take()}:
 * <ul>
 * <li>Obtain a database {@link Connection}.</li>
 * <li>Check if {@code ephemeral size} is full or not.</li>
 * <li>Call {@link #_takeWithRetries(Connection, int, int)}; which, in turn:
 * <ul>
 * <li>Start a database transaction.</li>
 * <li>Read a message off queue storage by calling {@link #peekFromQueueStorage(Connection)}.</li>
 * <li>Remove the message from queue (call {@link #removeFromQueueStorage(Connection, IQueueMessage)}) and out to ephemeral storage (call {@link #putToEphemeralStorage(Connection, IQueueMessage)}).</li>
 * <li>Commit transaction if all above operations were successful, rollback transaction otherwise.</li>
 * </ul>
 * </li>
 * </ul>
 * </li>
 * <li>{@link #finish(IQueueMessage)}:
 * <ul>
 * <li>Obtain a database {@link Connection}.</li>
 * <li>Call {@link #_finishWithRetries(Connection, IQueueMessage, int, int)}; which, in turn:
 * <ul>
 * <li>Call {@link #removeFromEphemeralStorage(Connection, IQueueMessage)}.</li>
 * </ul>
 * </li>
 * </ul>
 * </li>
 * <li>{@link #getOrphanMessages(long)}:
 * <ul>
 * <li>Obtain a database {@link Connection}.</li>
 * <li>Call {@link #_getOrphanMessagesWithRetries(long, Connection, int, int)}; which, in turn:
 * <ul>
 * <li>Start a database transaction.</li>
 * <li>Call {@link #getOrphanMessagesFromEphemeralStorage(Connection, long)}.</li>
 * <li>Commit transaction.</li>
 * </ul>
 * </li>
 * </ul>
 * </li>
 * <li>{@link #queue(IQueueMessage)}, {@link #requeue(IQueueMessage)} and {@link #requeueSilent(IQueueMessage)}: inherit from {@link AbstractQueue} and override {@link #doPutToQueue(IQueueMessage, PutToQueueCase)}
 * <ul>
 * <li>If {@code queueCase} parameter is {@link AbstractQueue.PutToQueueCase#REQUEUE}, call {@link #_requeueWithRetries(Connection, IQueueMessage, int, int)}; which, in turn:
 * <ul>
 * <li>Start a database transactions.</li>
 * <li>Call {@link #removeFromEphemeralStorage(Connection, IQueueMessage)}.</li>
 * <li>Call {@link #putToQueueStorage(Connection, IQueueMessage)}.</li>
 * <li>Commit transaction.</li>
 * </ul>
 * </li>
 * <li>If {@code queueCase} parameter is {@link AbstractQueue.PutToQueueCase#REQUEUE_SILENT}, call {@link #_requeueSilentWithRetries(Connection, IQueueMessage, int, int)}; which, in turn:
 * <ul>
 * <li>Start a database transactions.</li>
 * <li>Call {@link #removeFromEphemeralStorage(Connection, IQueueMessage)}.</li>
 * <li>Call {@link #putToQueueStorage(Connection, IQueueMessage)}.</li>
 * <li>Commit transaction.</li>
 * </ul>
 * </li>
 * <li>Otherwise, call {@link #_queueWithRetries(Connection, IQueueMessage, int, int)}; which, in turn:
 * <ul>
 * <li>Call {@link #putToQueueStorage(Connection, IQueueMessage)}</li>
 * </ul>
 * </li>
 * </ul>
 * </li>
 * </ul>
 * </p>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public abstract class JdbcQueue<ID, DATA> extends AbstractEphemeralSupportQueue<ID, DATA> {

    public final static int DEFAULT_MAX_RETRIES = 3;
    public final static int DEFAULT_TRANX_ISOLATION_LEVEL = Connection.TRANSACTION_READ_COMMITTED;

    private Logger LOGGER = LoggerFactory.getLogger(JdbcQueue.class);

    private final static String FIELD_COUNT = "num_entries";
    private String tableName, tableNameEphemeral;
    private String SQL_COUNT = "SELECT COUNT(*) AS " + FIELD_COUNT + " FROM {0}";
    private String SQL_COUNT_EPHEMERAL = "SELECT COUNT(*) AS " + FIELD_COUNT + " FROM {0}";

    private DataSource dataSource;
    private IJdbcHelper jdbcHelper;
    private boolean myOwnJdbcHelper = false;
    private int maxRetries = DEFAULT_MAX_RETRIES;
    private int transactionIsolationLevel = DEFAULT_TRANX_ISOLATION_LEVEL;

    /*----------------------------------------------------------------------*/

    /**
     * Name of database table to store queue messages.
     *
     * @param tableName
     * @return
     */
    public JdbcQueue<ID, DATA> setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * Name of database table to store queue messages.
     *
     * @return
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Name of database table to store ephemeral messages.
     *
     * @param tableNameEphemeral
     * @return
     */
    public JdbcQueue<ID, DATA> setTableNameEphemeral(String tableNameEphemeral) {
        this.tableNameEphemeral = tableNameEphemeral;
        return this;
    }

    /**
     * Name of database table to store ephemeral messages.
     *
     * @return
     */
    public String getTableNameEphemeral() {
        return tableNameEphemeral;
    }

    /**
     * Transaction isolation level used in DB-operations.
     *
     * @param transactionIsolationLevel
     * @return
     */
    public JdbcQueue<ID, DATA> setTransactionIsolationLevel(int transactionIsolationLevel) {
        this.transactionIsolationLevel = transactionIsolationLevel;
        return this;
    }

    /**
     * Transaction isolation level used in DB-operations.
     *
     * @return
     */
    public int getTransactionIsolationLevel() {
        return transactionIsolationLevel;
    }

    /**
     * Getter for {@link #jdbcHelper}.
     *
     * @return
     * @since 0.5.0
     */
    public IJdbcHelper getJdbcHelper() {
        return jdbcHelper;
    }

    /**
     * Setter for {@link #jdbcHelper}.
     *
     * @param jdbcHelper
     * @param setMyOwnJdbcHelper
     * @return
     * @since 0.7.1
     */
    protected JdbcQueue<ID, DATA> setJdbcHelper(IJdbcHelper jdbcHelper, boolean setMyOwnJdbcHelper) {
        if (this.jdbcHelper != null && myOwnJdbcHelper && this.jdbcHelper instanceof AbstractJdbcHelper) {
            ((AbstractJdbcHelper) this.jdbcHelper).destroy();
        }
        this.jdbcHelper = jdbcHelper;
        myOwnJdbcHelper = setMyOwnJdbcHelper;
        return this;
    }

    /**
     * Setter for {@link #jdbcHelper}.
     *
     * @param jdbcHelper
     * @return
     * @since 0.5.1.1
     */
    public JdbcQueue<ID, DATA> setJdbcHelper(IJdbcHelper jdbcHelper) {
        return setJdbcHelper(jdbcHelper, false);
    }

    /**
     * Getter for {@link #dataSource}.
     *
     * @return
     * @since 0.6.2.5
     */
    public DataSource getDataSource() {
        return dataSource;
    }

    /**
     * Setter for {@link #dataSource}.
     *
     * @param dataSource
     * @return
     * @since 0.6.2.5
     */
    public JdbcQueue<ID, DATA> setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    /**
     * Max number of retires for DB-operations.
     *
     * @param maxRetries
     * @return
     */
    public JdbcQueue<ID, DATA> setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }

    /**
     * Max number of retires for DB-operations.
     *
     * @return
     */
    public int getMaxRetries() {
        return maxRetries;
    }

    /*----------------------------------------------------------------------*/

    /**
     * Build an {@link IJdbcHelper} to be used by this JDBC queue.
     *
     * @return
     * @since 0.6.2.6
     */
    protected IJdbcHelper buildJdbcHelper() {
        if (dataSource == null) {
            throw new IllegalStateException("Data source is null.");
        }
        DdthJdbcHelper jdbcHelper = new DdthJdbcHelper();
        jdbcHelper.setDataSource(getDataSource()).init();
        return jdbcHelper;
    }

    /**
     * {@inheritDoc}
     *
     * @throws Exception
     */
    @Override
    public JdbcQueue<ID, DATA> init() throws Exception {
        SQL_COUNT = MessageFormat.format(SQL_COUNT, getTableName());
        SQL_COUNT_EPHEMERAL = MessageFormat.format(SQL_COUNT_EPHEMERAL, getTableNameEphemeral());

        if (jdbcHelper == null) {
            setJdbcHelper(buildJdbcHelper(), true);
        }

        super.init();

        if (jdbcHelper == null) {
            throw new IllegalStateException("JDBC helper is null.");
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        try {
            super.destroy();
        } finally {
            if (myOwnJdbcHelper && jdbcHelper != null && jdbcHelper instanceof AbstractJdbcHelper) {
                ((AbstractJdbcHelper) jdbcHelper).destroy();
            }
        }
    }

    /*----------------------------------------------------------------------*/

    /**
     * Primitive operation: Read (but not removed) message from head of queue storage.
     *
     * @param conn
     * @return
     */
    protected abstract IQueueMessage<ID, DATA> peekFromQueueStorage(Connection conn);

    /**
     * Primitive operation: Read (but not removed) a message from the ephemeral storage.
     *
     * @param conn
     * @param msg
     * @return
     * @since 0.2.1
     * @deprecated deprecated since 1.0.0, use {@link #readFromEphemeralStorage(Connection, Object)}
     */
    protected IQueueMessage<ID, DATA> readFromEphemeralStorage(Connection conn, IQueueMessage<ID, DATA> msg) {
        return readFromEphemeralStorage(conn, msg.getId());
    }

    /**
     * Primitive operation: Read (but not removed) a message from the ephemeral storage.
     *
     * @param conn
     * @param id
     * @return
     * @since 1.0.0
     */
    protected abstract IQueueMessage<ID, DATA> readFromEphemeralStorage(Connection conn, ID id);

    /**
     * Primitive operation: Get all orphan messages (messages that were left in ephemeral storage for
     * a long time).
     *
     * @param conn
     * @param thresholdTimestampMs get all orphan messages that were queued
     *                             <strong>before</strong> this timestamp
     * @return
     * @since 0.2.0
     */
    protected abstract Collection<? extends IQueueMessage<ID, DATA>> getOrphanMessagesFromEphemeralStorage(
            Connection conn, long thresholdTimestampMs);

    /**
     * Primitive operation: Put a message to tail of the queue storage.
     *
     * @param conn
     * @param msg
     * @return
     */
    protected abstract boolean putToQueueStorage(Connection conn, IQueueMessage<ID, DATA> msg);

    /**
     * Primitive operation: Put a message to the ephemeral storage.
     *
     * @param conn
     * @param msg
     */
    protected abstract boolean putToEphemeralStorage(Connection conn, IQueueMessage<ID, DATA> msg);

    /**
     * Primitive operation: Remove a message from the queue storage.
     *
     * @param conn
     * @param msg
     * @return
     */
    protected abstract boolean removeFromQueueStorage(Connection conn, IQueueMessage<ID, DATA> msg);

    /**
     * Primitive operation: Remove a message from the queue storage.
     *
     * @param conn
     * @param msg
     * @return
     */
    protected abstract boolean removeFromEphemeralStorage(Connection conn, IQueueMessage<ID, DATA> msg);

    /**
     * Execute a query, retry if deadlock.
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
     * @param numRetries
     * @param maxRetries
     * @param transactionMode
     * @param conn
     * @param sql
     * @param params
     * @return number of affected rows
     * @since 1.0.0
     */
    protected int executeWithRetries(int numRetries, int maxRetries, boolean transactionMode, Connection conn,
            String sql, Map<String, Object> params) {
        try {
            if (transactionMode) {
                jdbcHelper.startTransaction(conn);
                conn.setTransactionIsolation(transactionIsolationLevel);
            }
            int numRows = jdbcHelper.execute(conn, sql, params);
            if (transactionMode) {
                jdbcHelper.commitTransaction(conn);
            }
            return numRows;
        } catch (DuplicatedValueException dve) {
            LOGGER.warn(dve.getMessage(), dve);
            if (transactionMode) {
                jdbcHelper.rollbackTransaction(conn);
            }
            return 1;
        } catch (DaoException de) {
            if (de.getCause() instanceof DuplicateKeyException) {
                LOGGER.warn(de.getMessage(), de);
                if (transactionMode) {
                    jdbcHelper.rollbackTransaction(conn);
                }
                return 1;
            }
            if (de.getCause() instanceof ConcurrencyFailureException) {
                if (transactionMode) {
                    jdbcHelper.rollbackTransaction(conn);
                }
                if (numRetries > maxRetries) {
                    throw new QueueException(de);
                } else {
                    return executeWithRetries(numRetries + 1, maxRetries, transactionMode, conn, sql, params);
                }
            }
            if (transactionMode) {
                jdbcHelper.rollbackTransaction(conn);
            }
            throw de;
        } catch (Exception e) {
            if (transactionMode) {
                jdbcHelper.rollbackTransaction(conn);
            }
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * Execute a query, retry if deadlock.
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
     * @param numRetries
     * @param maxRetries
     * @param transactionMode
     * @param conn
     * @param sql
     * @param params
     * @return number of affected rows
     * @since 1.0.0
     */
    protected int executeWithRetries(int numRetries, int maxRetries, boolean transactionMode, Connection conn,
            String sql, Object... params) {
        try {
            if (transactionMode) {
                jdbcHelper.startTransaction(conn);
                conn.setTransactionIsolation(transactionIsolationLevel);
            }
            int numRows = jdbcHelper.execute(conn, sql, params);
            if (transactionMode) {
                jdbcHelper.commitTransaction(conn);
            }
            return numRows;
        } catch (DuplicatedValueException dve) {
            LOGGER.warn(dve.getMessage(), dve);
            if (transactionMode) {
                jdbcHelper.rollbackTransaction(conn);
            }
            return 1;
        } catch (DaoException de) {
            if (de.getCause() instanceof DuplicateKeyException) {
                LOGGER.warn(de.getMessage(), de);
                if (transactionMode) {
                    jdbcHelper.rollbackTransaction(conn);
                }
                return 1;
            }
            if (de.getCause() instanceof ConcurrencyFailureException) {
                if (transactionMode) {
                    jdbcHelper.rollbackTransaction(conn);
                }
                if (numRetries > maxRetries) {
                    throw new QueueException(de);
                } else {
                    return executeWithRetries(numRetries + 1, maxRetries, transactionMode, conn, sql, params);
                }
            }
            if (transactionMode) {
                jdbcHelper.rollbackTransaction(conn);
            }
            throw de;
        } catch (Exception e) {
            if (transactionMode) {
                jdbcHelper.rollbackTransaction(conn);
            }
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * Execute queries, retry if deadlock.
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
     * @param numRetries
     * @param maxRetries
     * @param sqlRunner
     * @param <T>
     * @return
     * @since 1.0.0
     */
    protected <T> T executeWithRetries(int numRetries, int maxRetries, Supplier<T> sqlRunner) {
        try {
            return sqlRunner.get();
        } catch (DaoException | QueueException e) {
            if (e.getCause() instanceof ConcurrencyFailureException) {
                if (numRetries > maxRetries) {
                    throw new QueueException(e);
                } else {
                    return executeWithRetries(numRetries + 1, maxRetries, sqlRunner);
                }
            }
            throw e;
        } catch (Exception e) {
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

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
     * @param conn
     * @param immutableMsg
     * @param numRetries
     * @param maxRetries
     * @return
     */
    protected boolean _queueWithRetries(Connection conn, IQueueMessage<ID, DATA> immutableMsg, int numRetries,
            int maxRetries) {
        try {
            return putToQueueStorage(conn, immutableMsg);
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
                    return _queueWithRetries(conn, immutableMsg, numRetries + 1, maxRetries);
                }
            }
            throw de;
        } catch (Exception e) {
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * Re-queue (silentlt) a message, retry if deadlock.
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
     * @param immutableMsg
     * @param numRetries
     * @param maxRetries
     * @return
     */
    protected boolean _requeueSilentWithRetries(Connection conn, IQueueMessage<ID, DATA> immutableMsg, int numRetries,
            int maxRetries) {
        try {
            jdbcHelper.startTransaction(conn);
            conn.setTransactionIsolation(transactionIsolationLevel);
            if (!isEphemeralDisabled()) {
                removeFromEphemeralStorage(conn, immutableMsg);
            }
            boolean result = putToQueueStorage(conn, immutableMsg);
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
                    return _requeueSilentWithRetries(conn, immutableMsg, numRetries + 1, maxRetries);
                }
            }
            jdbcHelper.rollbackTransaction(conn);
            throw de;
        } catch (Exception e) {
            jdbcHelper.rollbackTransaction(conn);
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
     * @param conn
     * @param immutableMsg
     * @param numRetries
     * @param maxRetries
     * @return
     */
    protected boolean _requeueWithRetries(Connection conn, IQueueMessage<ID, DATA> immutableMsg, int numRetries,
            int maxRetries) {
        try {
            jdbcHelper.startTransaction(conn);
            conn.setTransactionIsolation(transactionIsolationLevel);
            if (!isEphemeralDisabled()) {
                removeFromEphemeralStorage(conn, immutableMsg);
            }
            boolean result = putToQueueStorage(conn, immutableMsg);
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
                    return _requeueWithRetries(conn, immutableMsg, numRetries + 1, maxRetries);
                }
            }
            jdbcHelper.rollbackTransaction(conn);
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
    protected boolean doPutToQueue(IQueueMessage<ID, DATA> msg, PutToQueueCase queueCase) {
        try {
            try (Connection conn = jdbcHelper.getConnection()) {
                if (queueCase == PutToQueueCase.REQUEUE) {
                    return _requeueWithRetries(conn, msg, 0, this.maxRetries);
                } else if (queueCase == PutToQueueCase.REQUEUE_SILENT) {
                    return _requeueSilentWithRetries(conn, msg, 0, this.maxRetries);
                } else {
                    return _queueWithRetries(conn, msg, 0, this.maxRetries);
                }
            }
        } catch (Exception e) {
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
    protected void _finishWithRetries(Connection conn, IQueueMessage<ID, DATA> msg, int numRetries, int maxRetries) {
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
        try {
            try (Connection conn = jdbcHelper.getConnection()) {
                _finishWithRetries(conn, msg, 0, this.maxRetries);
            }
        } catch (Exception e) {
            final String logMsg = "(finish) Exception [" + e.getClass().getName() + "]: " + e.getMessage();
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
    protected IQueueMessage<ID, DATA> _takeWithRetries(Connection conn, int numRetries, int maxRetries) {
        try {
            jdbcHelper.startTransaction(conn);
            conn.setTransactionIsolation(transactionIsolationLevel);

            boolean result = true;
            IQueueMessage<ID, DATA> msg = peekFromQueueStorage(conn);
            if (msg != null) {
                result &= removeFromQueueStorage(conn, msg);
                if (!isEphemeralDisabled()) {
                    try {
                        result &= putToEphemeralStorage(conn, msg);
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
            jdbcHelper.rollbackTransaction(conn);
            throw de;
        } catch (Exception e) {
            jdbcHelper.rollbackTransaction(conn);
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     *
     * @throws QueueException.EphemeralIsFull if the ephemeral storage is full
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
    protected Collection<? extends IQueueMessage<ID, DATA>> _getOrphanMessagesWithRetries(long thresholdTimestampMs,
            Connection conn, int numRetries, int maxRetries) {
        try {
            jdbcHelper.startTransaction(conn);
            conn.setTransactionIsolation(transactionIsolationLevel);
            Collection<? extends IQueueMessage<ID, DATA>> msgs = getOrphanMessagesFromEphemeralStorage(conn,
                    thresholdTimestampMs);
            jdbcHelper.commitTransaction(conn);
            return msgs;
        } catch (DaoException de) {
            if (de.getCause() instanceof ConcurrencyFailureException) {
                jdbcHelper.rollbackTransaction(conn);
                if (numRetries > maxRetries) {
                    throw new QueueException(de);
                } else {
                    return _getOrphanMessagesWithRetries(thresholdTimestampMs, conn, numRetries + 1, maxRetries);
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
        Collection<IQueueMessage<ID, DATA>> orphanMessages = new HashSet<>();
        if (!isEphemeralDisabled()) {
            try (Connection conn = jdbcHelper.getConnection()) {
                orphanMessages.addAll(_getOrphanMessagesWithRetries(thresholdTimestampMs, conn, 0, this.maxRetries));
            } catch (Exception e) {
                throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
            }
        }
        return orphanMessages;
    }

    /**
     * @param conn
     * @param sql
     * @param field
     * @param params
     * @return
     * @since 1.0.0
     */
    protected int doSize(Connection conn, String sql, String field, Object... params) {
        Map<String, Object> row = jdbcHelper.executeSelectOne(conn, sql, params);
        return DPathUtils.getValueOptional(row, field, Integer.class).orElse(0).intValue();
    }

    /**
     * Get number of items currently in queue storage.
     *
     * @param conn
     * @return
     * @since 0.5.0
     */
    protected int queueSize(Connection conn) {
        return doSize(conn, SQL_COUNT, FIELD_COUNT);
    }

    /**
     * Get number of items currently in ephemeral storage.
     *
     * @param conn
     * @return
     * @since 0.5.0
     */
    protected int ephemeralSize(Connection conn) {
        return doSize(conn, SQL_COUNT_EPHEMERAL, FIELD_COUNT);
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

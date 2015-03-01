package con.github.ddth.queue.impl;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Date;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DeadlockLoserDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.github.ddth.dao.jdbc.BaseJdbcDao;

import con.github.ddth.queue.IQueue;
import con.github.ddth.queue.IQueueMessage;
import con.github.ddth.queue.utils.QueueException;

/**
 * Abstract JDBC implementation of {@link IQueue}.
 * 
 * <p>
 * Implementation:
 * <ul>
 * <li>Queue storage & Ephemeral storage are 2 database table, same structure.</li>
 * </ul>
 * </p>
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public abstract class JdbcQueue extends BaseJdbcDao implements IQueue {

    private Logger LOGGER = LoggerFactory.getLogger(JdbcQueue.class);

    private String tableName, tableNameEphemeral;
    private String SQL_COUNT = "SELECT COUNT(*) AS num_entries FROM {0}";
    private String SQL_COUNT_EPHEMERAL = "SELECT COUNT(*) AS num_entries FROM {0}";

    private static int MAX_RETRIES = 3;

    /*----------------------------------------------------------------------*/
    public JdbcQueue setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    protected String getTableName() {
        return tableName;
    }

    public JdbcQueue setTableNameEphemeral(String tableNameEphemeral) {
        this.tableNameEphemeral = tableNameEphemeral;
        return this;
    }

    protected String getTableNameEphemeral() {
        return tableNameEphemeral;
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    public JdbcQueue init() {
        SQL_COUNT = MessageFormat.format(SQL_COUNT, tableName);
        SQL_COUNT_EPHEMERAL = MessageFormat.format(SQL_COUNT_EPHEMERAL, tableNameEphemeral);
        return (JdbcQueue) super.init();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void destroy() {
        super.destroy();
    }

    /*----------------------------------------------------------------------*/

    /**
     * Reads a message from head of queue storage.
     * 
     * @param jdbcTemplate
     * @return
     */
    protected abstract IQueueMessage readFromQueueStorage(JdbcTemplate jdbcTemplate);

    /**
     * Reads a message from ephemeral storage.
     * 
     * @param jdbcTemplate
     * @return
     */
    protected abstract IQueueMessage readFromEphemeralStorage(JdbcTemplate jdbcTemplate);

    /**
     * Puts message to tail of queue storage.
     * 
     * @param jdbcTemplate
     * @param msg
     * @return
     */
    protected abstract boolean putToQueueStorage(JdbcTemplate jdbcTemplate, IQueueMessage msg);

    /**
     * Puts message to tail of ephemeral storage.
     * 
     * @param jdbcTemplate
     * @param msg
     * @return
     */
    protected abstract boolean putToEphemeralStorage(JdbcTemplate jdbcTemplate, IQueueMessage msg);

    /**
     * Removes message from queue storage.
     * 
     * @param jdbcTemplate
     * @param msg
     * @return
     */
    protected abstract boolean removeFromQueueStorage(JdbcTemplate jdbcTemplate, IQueueMessage msg);

    /**
     * Removes message from ephemeral storage.
     * 
     * @param jdbcTemplate
     * @param msg
     * @return
     */
    protected abstract boolean removeFromEphemeralStorage(JdbcTemplate jdbcTemplate,
            IQueueMessage msg);

    /**
     * Queues a message, retry if deadlock.
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
     * @param numRetries
     * @param maxRetries
     * @return
     */
    protected boolean queue(IQueueMessage msg, int numRetries, int maxRetries) {
        if (msg == null) {
            return false;
        }
        try {
            Connection conn = connection(true);
            try {
                conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                JdbcTemplate jdbcTemplate = jdbcTemplate(conn);
                Date now = new Date();
                msg.qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now);
                return putToQueueStorage(jdbcTemplate, msg);
            } finally {
                returnConnection(conn);
            }
        } catch (DeadlockLoserDataAccessException dle) {
            if (numRetries > maxRetries) {
                throw new QueueException(dle);
            } else {
                return queue(msg, numRetries + 1, maxRetries);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (e instanceof QueueException) {
                throw (QueueException) e;
            } else {
                throw new QueueException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean queue(IQueueMessage msg) {
        return queue(msg, 0, MAX_RETRIES);
    }

    /**
     * Re-queues a message, retry if deadlock.
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
     * @param numRetries
     * @param maxRetries
     * @return
     */
    protected boolean requeue(IQueueMessage msg, int numRetries, int maxRetries) {
        if (msg == null) {
            return false;
        }
        try {
            Connection conn = connection(true);
            try {
                conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                JdbcTemplate jdbcTemplate = jdbcTemplate(conn);

                removeFromEphemeralStorage(jdbcTemplate, msg);

                Date now = new Date();
                msg.qIncNumRequeues().qTimestamp(now);
                return putToQueueStorage(jdbcTemplate, msg);
            } finally {
                returnConnection(conn);
            }
        } catch (DeadlockLoserDataAccessException dle) {
            if (numRetries > maxRetries) {
                throw new QueueException(dle);
            } else {
                return requeueSilent(msg, numRetries + 1, maxRetries);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (e instanceof QueueException) {
                throw (QueueException) e;
            } else {
                throw new QueueException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeue(IQueueMessage msg) {
        return requeue(msg, 0, MAX_RETRIES);
    }

    /**
     * Re-queues a message silently, retry if deadlock.
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
     * @param numRetries
     * @param maxRetries
     * @return
     */
    protected boolean requeueSilent(IQueueMessage msg, int numRetries, int maxRetries) {
        if (msg == null) {
            return false;
        }
        try {
            Connection conn = connection(true);
            try {
                conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                JdbcTemplate jdbcTemplate = jdbcTemplate(conn);

                removeFromEphemeralStorage(jdbcTemplate, msg);
                return putToQueueStorage(jdbcTemplate, msg);
            } finally {
                returnConnection(conn);
            }
        } catch (DeadlockLoserDataAccessException dle) {
            if (numRetries > maxRetries) {
                throw new QueueException(dle);
            } else {
                return requeueSilent(msg, numRetries + 1, maxRetries);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (e instanceof QueueException) {
                throw (QueueException) e;
            } else {
                throw new QueueException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeueSilent(IQueueMessage msg) {
        return requeueSilent(msg, 0, MAX_RETRIES);
    }

    /**
     * Performs "finish" action, retry if deadlock.
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
     * @param numRetries
     * @param maxRetries
     */
    protected void finish(IQueueMessage msg, int numRetries, int maxRetries) {
        try {
            Connection conn = connection(true);
            try {
                conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                JdbcTemplate jdbcTemplate = jdbcTemplate(conn);

                removeFromEphemeralStorage(jdbcTemplate, msg);
            } finally {
                returnConnection(conn);
            }
        } catch (DeadlockLoserDataAccessException dle) {
            if (numRetries > maxRetries) {
                throw new QueueException(dle);
            } else {
                finish(msg, numRetries + 1, maxRetries);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (e instanceof QueueException) {
                throw (QueueException) e;
            } else {
                throw new QueueException(e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage msg) {
        finish(msg, 0, MAX_RETRIES);
    }

    /**
     * Takes a message from queue, retry if deadlock.
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
     * @return
     */
    protected IQueueMessage take(final int numRetries, final int maxRetries) {
        IQueueMessage msg = null;
        try {
            Connection conn = connection(true);
            try {
                conn.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
                JdbcTemplate jdbcTemplate = jdbcTemplate(conn);

                msg = readFromQueueStorage(jdbcTemplate);

                if (msg != null) {
                    removeFromQueueStorage(jdbcTemplate, msg);

                    putToEphemeralStorage(jdbcTemplate, msg);
                }
            } finally {
                returnConnection(conn);
            }
        } catch (DeadlockLoserDataAccessException dle) {
            if (numRetries > maxRetries) {
                throw new QueueException(dle);
            } else {
                finish(msg, numRetries + 1, maxRetries);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            if (e instanceof QueueException) {
                throw (QueueException) e;
            } else {
                throw new QueueException(e);
            }
        }
        return msg;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IQueueMessage take() {
        return take(0, MAX_RETRIES);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        try {
            Connection conn = connection();
            try {
                JdbcTemplate jdbcTemplate = jdbcTemplate(conn);
                Integer result = jdbcTemplate.queryForObject(SQL_COUNT, null, Integer.class);
                return result != null ? result.intValue() : 0;
            } finally {
                returnConnection(conn);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return -1;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        try {
            Connection conn = connection();
            try {
                JdbcTemplate jdbcTemplate = jdbcTemplate(conn);
                Integer result = jdbcTemplate.queryForObject(SQL_COUNT_EPHEMERAL, null,
                        Integer.class);
                return result != null ? result.intValue() : 0;
            } finally {
                returnConnection(conn);
            }
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            return -1;
        }
    }
}

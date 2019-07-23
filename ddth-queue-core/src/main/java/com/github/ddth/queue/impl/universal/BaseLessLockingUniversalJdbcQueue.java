package com.github.ddth.queue.impl.universal;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.commons.utils.MapUtils;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.utils.BuildNamedParamsSqlResult;
import com.github.ddth.dao.jdbc.utils.DefaultNamedParamsFilters;
import com.github.ddth.dao.jdbc.utils.DefaultNamedParamsSqlBuilders;
import com.github.ddth.dao.jdbc.utils.ParamRawExpression;
import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.internal.utils.QueueUtils;

/**
 * Same as {@link BaseUniversalJdbcQueue}, but using a "less-locking" algorithm,
 * requires only one single db table for both queue and ephemeral storage.
 *
 * <p>
 * Queue db table schema:
 * </p>
 * <ul>
 * <li>{@code queue_id}: {@code <ID>}, see {@link IQueueMessage#getId()}, {@link #COL_QUEUE_ID}</li>
 * <li>{@code ephemeral_id}: {@code bigint, not null, default value = 0}, see {@link #COL_EPHEMERAL_ID}</li>
 * <li>{@code msg_org_timestamp}: {@code datetime}, see {@link IQueueMessage#getTimestamp()}, {@link #COL_ORG_TIMESTAMP}</li>
 * <li>{@code msg_timestamp}: {@code datetime}, see {@link IQueueMessage#getQueueTimestamp()}, {@link #COL_TIMESTAMP}</li>
 * <li>{@code msg_num_requeues}: {@code int}, see {@link IQueueMessage#getNumRequeues()}, {@link #COL_NUM_REQUEUES}</li>
 * <li>{@code msg_content}: {@code blob}, message's content, see {@link #COL_CONTENT}</li>
 * </ul>
 *
 * @param <T>
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 1.0.0
 */
public abstract class BaseLessLockingUniversalJdbcQueue<T extends BaseUniversalQueueMessage<ID>, ID>
        extends BaseUniversalJdbcQueue<T, ID> {
    /**
     * Table's column name to store queue-id
     */
    public final static String COL_QUEUE_ID = "queue_id";

    /**
     * Table's column name to store ephemeral id
     */
    public final static String COL_EPHEMERAL_ID = "ephemeral_id";

    /**
     * Table's column name to store message's original timestamp
     */
    public final static String COL_ORG_TIMESTAMP = "msg_org_timestamp";

    /**
     * Table's column name to store message's timestamp
     */
    public final static String COL_TIMESTAMP = "msg_timestamp";

    /**
     * Table's column name to store message's number of requeues
     */
    public final static String COL_NUM_REQUEUES = "msg_num_requeues";

    /**
     * Table's column name to store message's content
     */
    public final static String COL_CONTENT = "msg_content";

    /**
     * {@inheritDoc}
     */
    @Override
    public String getTableNameEphemeral() {
        return getTableName();
    }

    /*----------------------------------------------------------------------*/

    private final static String FIELD_COUNT = "num_entries";
    private String SQL_COUNT = "SELECT COUNT(*) AS " + FIELD_COUNT + " FROM {0} WHERE " + COL_EPHEMERAL_ID + "=0";
    private String SQL_COUNT_EPHEMERAL =
            "SELECT COUNT(*) AS " + FIELD_COUNT + " FROM {0} WHERE " + COL_EPHEMERAL_ID + "!=0";
    private BuildNamedParamsSqlResult NPSQL_GET_ORPHAN_MSGS;
    private BuildNamedParamsSqlResult NPSQL_DELETE_MSG;
    private BuildNamedParamsSqlResult NPSQL_PUT_NEW_TO_QUEUE, NPSQL_REPUT_TO_QUEUE;
    private BuildNamedParamsSqlResult NPSQL_REQUEUE, NPSQL_REQUEUE_SILENT;
    private BuildNamedParamsSqlResult NPSQL_GET_FIRST_AVAILABLE_MSG, NPSQL_ASSIGN_EPHEMERAL_ID;

    /**
     * {@inheritDoc}
     */
    @Override
    public BaseLessLockingUniversalJdbcQueue<T, ID> init() throws Exception {
        /* count number of queue messages */
        SQL_COUNT = MessageFormat.format(SQL_COUNT, getTableName());
        /* count number of ephemeral messages */
        SQL_COUNT_EPHEMERAL = MessageFormat.format(SQL_COUNT_EPHEMERAL, getTableNameEphemeral());

        String[] COLUMNS_SELECT = { COL_QUEUE_ID + " AS " + UniversalIdStrQueueMessage.FIELD_QUEUE_ID,
                COL_ORG_TIMESTAMP + " AS " + UniversalIdStrQueueMessage.FIELD_TIMESTAMP,
                COL_TIMESTAMP + " AS " + UniversalIdStrQueueMessage.FIELD_QUEUE_TIMESTAMP,
                COL_NUM_REQUEUES + " AS " + UniversalIdStrQueueMessage.FIELD_NUM_REQUEUES,
                COL_CONTENT + " AS " + UniversalIdStrQueueMessage.FIELD_DATA };

        /* read orphan messages from ephemeral storage */
        NPSQL_GET_ORPHAN_MSGS = new DefaultNamedParamsSqlBuilders.SelectBuilder().withColumns(COLUMNS_SELECT)
                .withFilterWhere(new DefaultNamedParamsFilters.FilterAnd().addFilter(
                        new DefaultNamedParamsFilters.FilterFieldValue(COL_EPHEMERAL_ID, "!=",
                                new ParamRawExpression("0")))
                        .addFilter(new DefaultNamedParamsFilters.FilterFieldValue(COL_TIMESTAMP, "<", "dummy")))
                .withTableNames(getTableNameEphemeral()).build();

        /* remove a message from storage completely */
        NPSQL_DELETE_MSG = new DefaultNamedParamsSqlBuilders.DeleteBuilder(getTableName(),
                new DefaultNamedParamsFilters.FilterFieldValue(COL_QUEUE_ID, "=", "dummy")).build();

        /* put a new message (message without pre-set queue id) to queue, assuming column COL_QUEUE_ID is auto-number. */
        NPSQL_PUT_NEW_TO_QUEUE = new DefaultNamedParamsSqlBuilders.InsertBuilder(getTableName(),
                MapUtils.createMap(COL_EPHEMERAL_ID, "dummy", COL_ORG_TIMESTAMP, "dummy", COL_TIMESTAMP, "dummy",
                        COL_NUM_REQUEUES, "dummy", COL_CONTENT, "dummy")).build();
        /* put a message with pre-set queue id to queue */
        NPSQL_REPUT_TO_QUEUE = new DefaultNamedParamsSqlBuilders.InsertBuilder(getTableName(),
                MapUtils.createMap(COL_QUEUE_ID, "dummy", COL_EPHEMERAL_ID, "dummy", COL_ORG_TIMESTAMP, "dummy",
                        COL_TIMESTAMP, "dummy", COL_NUM_REQUEUES, "dummy", COL_CONTENT, "dummy")).build();

        /* requeue a message: move from ephemeral storage to queue storage by resetting value of COL_EPHEMERAL_ID */
        NPSQL_REQUEUE = new DefaultNamedParamsSqlBuilders.UpdateBuilder(getTableName()).withValues(
                MapUtils.createMap(COL_EPHEMERAL_ID, new ParamRawExpression("0"), COL_NUM_REQUEUES,
                        new ParamRawExpression(COL_NUM_REQUEUES + "+1"), COL_TIMESTAMP, "dummy")).withFilter(
                new DefaultNamedParamsFilters.FilterAnd()
                        .addFilter(new DefaultNamedParamsFilters.FilterFieldValue(COL_QUEUE_ID, "=", "dummy"))
                        .addFilter(new DefaultNamedParamsFilters.FilterFieldValue(COL_EPHEMERAL_ID, "!=",
                                new ParamRawExpression("0")))).build();
        /* requeue a message silently: move from ephemeral storage to queue storage by resetting value of COL_EPHEMERAL_ID */
        NPSQL_REQUEUE_SILENT = new DefaultNamedParamsSqlBuilders.UpdateBuilder(getTableName())
                .withValues(MapUtils.createMap(COL_EPHEMERAL_ID, new ParamRawExpression("0"))).withFilter(
                        new DefaultNamedParamsFilters.FilterAnd()
                                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue(COL_QUEUE_ID, "=", "dummy"))
                                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue(COL_EPHEMERAL_ID, "!=",
                                        new ParamRawExpression("0")))).build();

        /* get first available queue message */
        NPSQL_GET_FIRST_AVAILABLE_MSG = new DefaultNamedParamsSqlBuilders.SelectBuilder().withColumns(COLUMNS_SELECT)
                .withFilterWhere(new DefaultNamedParamsFilters.FilterFieldValue(COL_EPHEMERAL_ID, "=",
                        new ParamRawExpression("0"))).withLimit(1)
                .withSorting(isFifo() ? MapUtils.createMap(COL_ORG_TIMESTAMP, Boolean.FALSE) : null)
                .withTableNames(getTableName()).build();
        /* assign ephemeral-id to message */
        NPSQL_ASSIGN_EPHEMERAL_ID = new DefaultNamedParamsSqlBuilders.UpdateBuilder(getTableName())
                .withValues(MapUtils.createMap(COL_EPHEMERAL_ID, "dummy")).withFilter(
                        new DefaultNamedParamsFilters.FilterAnd()
                                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue(COL_QUEUE_ID, "=", "dummy"))
                                .addFilter(new DefaultNamedParamsFilters.FilterFieldValue(COL_EPHEMERAL_ID, "=",
                                        new ParamRawExpression("0")))).build();

        super.init();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int queueSize(Connection conn) {
        return doSize(conn, SQL_COUNT, FIELD_COUNT);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected int ephemeralSize(Connection conn) {
        return doSize(conn, SQL_COUNT_EPHEMERAL, FIELD_COUNT);
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    protected T peekFromQueueStorage(Connection conn) {
        // UNUSED
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected T readFromEphemeralStorage(Connection conn, ID id) {
        // UNUSED
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean putToEphemeralStorage(Connection conn, IQueueMessage<ID, byte[]> _msg) {
        // UNUSED
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean removeFromQueueStorage(Connection conn, IQueueMessage<ID, byte[]> msg) {
        // UNUSED
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean removeFromEphemeralStorage(Connection conn, IQueueMessage<ID, byte[]> msg) {
        // UNUSED
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collection<T> getOrphanMessagesFromEphemeralStorage(Connection conn, long thresholdTimestampMs) {
        Date threshold = new Date(System.currentTimeMillis() - thresholdTimestampMs);
        Map<String, Object> params = MapUtils.createMap(COL_TIMESTAMP, threshold);
        return selectMessages(conn, NPSQL_GET_ORPHAN_MSGS.clause, params);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean putToQueueStorage(Connection conn, IQueueMessage<ID, byte[]> msg) {
        ID qid = msg.getId();
        boolean isEmptyId = qid == null;
        if (!isEmptyId) {
            if (qid instanceof String) {
                isEmptyId = StringUtils.isBlank(qid.toString());
            }
            if (qid instanceof Number) {
                isEmptyId = ((Number) qid).longValue() == 0;
            }
        }
        int numRows;
        if (isEmptyId) {
            Map<String, Object> params = MapUtils
                    .createMap(COL_EPHEMERAL_ID, 0, COL_ORG_TIMESTAMP, msg.getTimestamp(), COL_TIMESTAMP,
                            msg.getQueueTimestamp(), COL_NUM_REQUEUES, msg.getNumRequeues(), COL_CONTENT,
                            msg.getData());
            numRows = getJdbcHelper().execute(conn, NPSQL_PUT_NEW_TO_QUEUE.clause, params);
        } else {
            Map<String, Object> params = MapUtils
                    .createMap(COL_QUEUE_ID, qid, COL_EPHEMERAL_ID, 0, COL_ORG_TIMESTAMP, msg.getTimestamp(),
                            COL_TIMESTAMP, msg.getQueueTimestamp(), COL_NUM_REQUEUES, msg.getNumRequeues(), COL_CONTENT,
                            msg.getData());
            numRows = getJdbcHelper().execute(conn, NPSQL_REPUT_TO_QUEUE.clause, params);
        }
        return numRows > 0;
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     *
     * <p>Implementation: remove message completely from storage.</p>
     */
    @Override
    protected void _finishWithRetries(Connection conn, IQueueMessage<ID, byte[]> msg, int numRetries, int maxRetries) {
        Map<String, Object> params = MapUtils.createMap(COL_QUEUE_ID, msg.getId());
        executeWithRetries(numRetries, maxRetries, false, conn, NPSQL_DELETE_MSG.clause, params);
    }

    /** There is no overridden implementation of {@link #_queueWithRetries(Connection, IQueueMessage, int, int)} as it has been covered by {@link #putToQueueStorage(Connection, IQueueMessage)} */

    /**
     * {@inheritDoc}
     *
     * <p>Implementation:</p>
     * <ul>
     * <li>Reset value of {@link #COL_EPHEMERAL_ID} to {@code 0}.</li>
     * <li>Update value of {@link #COL_NUM_REQUEUES} and {@link #COL_TIMESTAMP}.</li>
     * </ul>
     */
    @Override
    protected boolean _requeueWithRetries(Connection conn, IQueueMessage<ID, byte[]> msg, int numRetries,
            int maxRetries) {
        Map<String, Object> params = MapUtils.createMap(COL_QUEUE_ID, msg.getId(), COL_TIMESTAMP, new Date());
        int numRows = executeWithRetries(numRetries, maxRetries, false, conn, NPSQL_REQUEUE.clause, params);
        return numRows > 0;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Implementation:</p>
     * <ul>
     * <li>Reset value of {@link #COL_EPHEMERAL_ID} to {@code 0}.</li>
     * <li>Update value of {@link #COL_NUM_REQUEUES} and {@link #COL_TIMESTAMP}.</li>
     * </ul>
     */
    @Override
    protected boolean _requeueSilentWithRetries(Connection conn, IQueueMessage<ID, byte[]> msg, int numRetries,
            int maxRetries) {
        Map<String, Object> params = MapUtils.createMap(COL_QUEUE_ID, msg.getId());
        int numRows = executeWithRetries(numRetries, maxRetries, false, conn, NPSQL_REQUEUE_SILENT.clause, params);
        return numRows > 0;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Implementation:</p>
     * <ul>
     * <li>Get the first available queue message.</li>
     * <li>Generate a unique-id and assign to the message's {@link #COL_EPHEMERAL_ID}.</li>
     * <li>Return the message object.</li>
     * </ul>
     */
    @Override
    protected T _takeWithRetries(Connection conn, int numRetries, int maxRetries) {
        IJdbcHelper jdbcHelper = getJdbcHelper();
        return executeWithRetries(numRetries, maxRetries, () -> {
            try {
                jdbcHelper.startTransaction(conn);
                conn.setTransactionIsolation(getTransactionIsolationLevel());

                Map<String, Object> dbRow = jdbcHelper.executeSelectOne(conn, NPSQL_GET_FIRST_AVAILABLE_MSG.clause);
                T msg = dbRow != null ? createMessge(dbRow) : null;
                if (msg != null) {
                    long ephemeralId = QueueUtils.IDGEN.generateId64();
                    Map<String, Object> params = MapUtils
                            .createMap(COL_EPHEMERAL_ID, ephemeralId, COL_QUEUE_ID, msg.getId());
                    int numRows = jdbcHelper.execute(conn, NPSQL_ASSIGN_EPHEMERAL_ID.clause, params);
                    msg = numRows > 0 ? msg : null;
                }

                if (msg == null) {
                    jdbcHelper.rollbackTransaction(conn);
                } else {
                    jdbcHelper.commitTransaction(conn);
                }
                return msg;
            } catch (Exception e) {
                jdbcHelper.rollbackTransaction(conn);
                throw e instanceof DaoException ? (DaoException) e : new DaoException(e);
            }
        });
    }
}

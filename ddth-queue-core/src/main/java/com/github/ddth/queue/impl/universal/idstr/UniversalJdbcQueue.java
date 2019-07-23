package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.commons.utils.MapUtils;
import com.github.ddth.dao.jdbc.utils.DefaultNamedParamsFilters;
import com.github.ddth.dao.jdbc.utils.DefaultNamedParamsSqlBuilders;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.BaseUniversalJdbcQueue;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessageFactory;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Universal JDBC implementation of {@link IQueue}.
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
 * <li>{@code msg_org_timestamp}: {@code datetime}, see {@link IQueueMessage#getTimestamp()}, {@link #COL_ORG_TIMESTAMP}</li>
 * <li>{@code msg_timestamp}: {@code datetime}, see {@link IQueueMessage#getQueueTimestamp()}, {@link #COL_TIMESTAMP}</li>
 * <li>{@code msg_num_requeues}: {@code int}, see {@link IQueueMessage#getNumRequeues()}, {@link #COL_NUM_REQUEUES}</li>
 * <li>{@code msg_content}: {@code blob}, message's content, see {@link #COL_CONTENT}</li>
 * </ul>
 *
 * <p>
 * Ephemeral db table schema:
 * </p>
 * <ul>
 * <li>{@code queue_id}: {@code varchar(32)}, see {@link IQueueMessage#getId()}, {@link #COL_QUEUE_ID}</li>
 * <li>{@code msg_org_timestamp}: {@code datetime}, see {@link IQueueMessage#getTimestamp()}, {@link #COL_ORG_TIMESTAMP}</li>
 * <li>{@code msg_timestamp}: {@code datetime}, see {@link IQueueMessage#getQueueTimestamp()}, {@link #COL_TIMESTAMP}</li>
 * <li>{@code msg_num_requeues}: {@code int}, see {@link IQueueMessage#getNumRequeues()}, {@link #COL_NUM_REQUEUES}</li>
 * <li>{@code msg_content}: {@code blob}, message's content, see {@link #COL_CONTENT}</li>
 * </ul>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.3
 */
public class UniversalJdbcQueue extends BaseUniversalJdbcQueue<UniversalIdStrQueueMessage, String> {
    /**
     * Table's column name to store queue-id
     */
    public final static String COL_QUEUE_ID = "queue_id";

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

    /*----------------------------------------------------------------------*/

    private String SQL_PEEK_FROM_QUEUE, SQL_READ_FROM_EPHEMERAL;
    private String SQL_GET_ORPHAN_MSGS;
    private String SQL_PUT_NEW_TO_QUEUE, SQL_REPUT_TO_QUEUE, SQL_PUT_TO_EPHEMERAL;
    private String SQL_REMOVE_FROM_QUEUE, SQL_REMOVE_FROM_EPHEMERAL;

    /**
     * {@inheritDoc}
     */
    @Override
    public UniversalJdbcQueue init() throws Exception {
        String[] COLS_SELECT = { COL_QUEUE_ID + " AS " + UniversalIdStrQueueMessage.FIELD_QUEUE_ID,
                COL_ORG_TIMESTAMP + " AS " + UniversalIdStrQueueMessage.FIELD_TIMESTAMP,
                COL_TIMESTAMP + " AS " + UniversalIdStrQueueMessage.FIELD_QUEUE_TIMESTAMP,
                COL_NUM_REQUEUES + " AS " + UniversalIdStrQueueMessage.FIELD_NUM_REQUEUES,
                COL_CONTENT + " AS " + UniversalIdStrQueueMessage.FIELD_DATA };

        /* peek a message off a queue */
        SQL_PEEK_FROM_QUEUE = new DefaultNamedParamsSqlBuilders.SelectBuilder().withColumns(COLS_SELECT)
                .withSorting(isFifo() ? MapUtils.createMap(COL_ORG_TIMESTAMP, Boolean.FALSE) : null).withLimit(1)
                .withTableNames(getTableName()).build().clause;
        /* read a message from ephemeral storage */
        SQL_READ_FROM_EPHEMERAL = new DefaultNamedParamsSqlBuilders.SelectBuilder().withColumns(COLS_SELECT)
                .withFilterWhere(new DefaultNamedParamsFilters.FilterFieldValue(COL_QUEUE_ID, "=", "dummy"))
                .withTableNames(getTableNameEphemeral()).build().clause;

        /* read orphan messages from ephemeral storage */
        SQL_GET_ORPHAN_MSGS = new DefaultNamedParamsSqlBuilders.SelectBuilder().withColumns(COLS_SELECT)
                .withFilterWhere(new DefaultNamedParamsFilters.FilterFieldValue(COL_TIMESTAMP, "<", "dummy"))
                .withTableNames(getTableNameEphemeral()).build().clause;

        /* put a new message (message without pre-set queue id) to queue, assuming column COL_QUEUE_ID is auto-number. */
        SQL_PUT_NEW_TO_QUEUE = new DefaultNamedParamsSqlBuilders.InsertBuilder(getTableName(),
                MapUtils.createMap(COL_ORG_TIMESTAMP, "dummy", COL_TIMESTAMP, "dummy", COL_NUM_REQUEUES, "dummy",
                        COL_CONTENT, "dummy")).build().clause;
        /* put a message with pre-set queue id to queue */
        SQL_REPUT_TO_QUEUE = new DefaultNamedParamsSqlBuilders.InsertBuilder(getTableName(),
                MapUtils.createMap(COL_QUEUE_ID, "dummy", COL_ORG_TIMESTAMP, "dummy", COL_TIMESTAMP, "dummy",
                        COL_NUM_REQUEUES, "dummy", COL_CONTENT, "dummy")).build().clause;
        /* put a message to ephemeral storage */
        SQL_PUT_TO_EPHEMERAL = new DefaultNamedParamsSqlBuilders.InsertBuilder(getTableNameEphemeral(),
                MapUtils.createMap(COL_QUEUE_ID, "dummy", COL_ORG_TIMESTAMP, "dummy", COL_TIMESTAMP, "dummy",
                        COL_NUM_REQUEUES, "dummy", COL_CONTENT, "dummy")).build().clause;

        /* remove a message from queue */
        SQL_REMOVE_FROM_QUEUE = new DefaultNamedParamsSqlBuilders.DeleteBuilder(getTableName(),
                new DefaultNamedParamsFilters.FilterFieldValue(COL_QUEUE_ID, "=", "dummy")).build().clause;
        /* remove a message from ephemeral storage */
        SQL_REMOVE_FROM_EPHEMERAL = new DefaultNamedParamsSqlBuilders.DeleteBuilder(getTableNameEphemeral(),
                new DefaultNamedParamsFilters.FilterFieldValue(COL_QUEUE_ID, "=", "dummy")).build().clause;

        if (getMessageFactory() == null) {
            setMessageFactory(UniversalIdStrQueueMessageFactory.INSTANCE);
        }
        super.init();
        return this;
    }

    /*----------------------------------------------------------------------*/

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdStrQueueMessage peekFromQueueStorage(Connection conn) {
        Map<String, Object> dbRow = getJdbcHelper().executeSelectOne(conn, SQL_PEEK_FROM_QUEUE);
        return dbRow != null ? createMessge(dbRow) : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdStrQueueMessage readFromEphemeralStorage(Connection conn, String id) {
        Map<String, Object> dbRow = getJdbcHelper()
                .executeSelectOne(conn, SQL_READ_FROM_EPHEMERAL, MapUtils.createMap(COL_QUEUE_ID, id));
        return dbRow != null ? createMessge(dbRow) : null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected Collection<UniversalIdStrQueueMessage> getOrphanMessagesFromEphemeralStorage(Connection conn,
            long thresholdTimestampMs) {
        Date threshold = new Date(System.currentTimeMillis() - thresholdTimestampMs);
        Collection<UniversalIdStrQueueMessage> result = new ArrayList<>();
        try (Stream<Map<String, Object>> dbRows = getJdbcHelper()
                .executeSelectAsStream(conn, SQL_GET_ORPHAN_MSGS, MapUtils.createMap(COL_TIMESTAMP, threshold))) {
            dbRows.forEach(row -> result.add(UniversalIdStrQueueMessage.newInstance(row)));
        }
        return result;
    }

    private Map<String, Object> toMapForSqlBuilder(UniversalIdStrQueueMessage msg) {
        return MapUtils.createMap(COL_QUEUE_ID, msg.getId(), COL_ORG_TIMESTAMP, msg.getTimestamp(), COL_TIMESTAMP,
                msg.getQueueTimestamp(), COL_NUM_REQUEUES, msg.getNumRequeues(), COL_CONTENT, msg.getContent());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean putToQueueStorage(Connection conn, IQueueMessage<String, byte[]> _msg) {
        UniversalIdStrQueueMessage msg = ensureMessageType(_msg, UniversalIdStrQueueMessage.class);
        String qid = msg.getId();
        if (StringUtils.isBlank(qid)) {
            int numRows = getJdbcHelper().execute(conn, SQL_PUT_NEW_TO_QUEUE, toMapForSqlBuilder(msg));
            return numRows > 0;
        } else {
            int numRows = getJdbcHelper().execute(conn, SQL_REPUT_TO_QUEUE, toMapForSqlBuilder(msg));
            return numRows > 0;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean putToEphemeralStorage(Connection conn, IQueueMessage<String, byte[]> _msg) {
        UniversalIdStrQueueMessage msg = ensureMessageType(_msg, UniversalIdStrQueueMessage.class);
        int numRows = getJdbcHelper().execute(conn, SQL_PUT_TO_EPHEMERAL, toMapForSqlBuilder(msg));
        return numRows > 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean removeFromQueueStorage(Connection conn, IQueueMessage<String, byte[]> _msg) {
        UniversalIdStrQueueMessage msg = ensureMessageType(_msg, UniversalIdStrQueueMessage.class);
        int numRows = getJdbcHelper()
                .execute(conn, SQL_REMOVE_FROM_QUEUE, MapUtils.createMap(COL_QUEUE_ID, msg.getId()));
        return numRows > 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean removeFromEphemeralStorage(Connection conn, IQueueMessage<String, byte[]> _msg) {
        UniversalIdStrQueueMessage msg = ensureMessageType(_msg, UniversalIdStrQueueMessage.class);
        int numRows = getJdbcHelper()
                .execute(conn, SQL_REMOVE_FROM_EPHEMERAL, MapUtils.createMap(COL_QUEUE_ID, msg.getId()));
        return numRows > 0;
    }
}

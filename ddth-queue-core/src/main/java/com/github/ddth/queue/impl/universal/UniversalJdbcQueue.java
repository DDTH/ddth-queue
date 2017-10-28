package com.github.ddth.queue.impl.universal;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.JdbcQueue;

/**
 * Universal JDBC implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalQueueMessage}s.
 * </p>
 * 
 * <p>
 * {@code ephemeralDisabled}: when set to {@code true}, ephemeral storage is
 * disabled. Default value is {@code false}
 * </p>
 * 
 * <p>
 * {@code fifo}: when set to {@code true} (which is default) messages are taken
 * in FIFO manner.
 * </p>
 * 
 * <p>
 * Queue db table schema:
 * </p>
 * <ul>
 * <li>{@code queue_id}: {@code bigint, auto increment}, see
 * {@link IQueueMessage#qId()}, {@link #COL_QUEUE_ID}</li>
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
 * <p>
 * Ephemeral db table schema:
 * </p>
 * <ul>
 * <li>{@code queue_id}: {@code bigint}, see {@link IQueueMessage#qId()},
 * {@link #COL_QUEUE_ID}</li>
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
public class UniversalJdbcQueue extends JdbcQueue {

    /** Table's column name to store queue-id */
    public final static String COL_QUEUE_ID = "queue_id";

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
    public UniversalJdbcQueue setFifo(boolean fifo) {
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
    public UniversalJdbcQueue markFifo(boolean fifo) {
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

    /*----------------------------------------------------------------------*/

    private String SQL_READ_FROM_QUEUE, SQL_READ_FROM_EPHEMERAL;
    private String SQL_GET_ORPHAN_MSGS;
    private String SQL_PUT_NEW_TO_QUEUE, SQL_REPUT_TO_QUEUE, SQL_PUT_TO_EPHEMERAL;
    private String SQL_REMOVE_FROM_QUEUE, SQL_REMOVE_FROM_EPHEMERAL;

    public UniversalJdbcQueue init() {
        super.init();

        /*
         * Takes a message from queue
         */
        SQL_READ_FROM_QUEUE = "SELECT {1}, {2}, {3}, {4}, {5} FROM {0}"
                + (fifo ? (" ORDER BY " + COL_QUEUE_ID) : "") + " LIMIT 1";
        SQL_READ_FROM_QUEUE = MessageFormat.format(SQL_READ_FROM_QUEUE, getTableName(),
                COL_QUEUE_ID + " AS " + UniversalQueueMessage.FIELD_QUEUE_ID,
                COL_ORG_TIMESTAMP + " AS " + UniversalQueueMessage.FIELD_ORG_TIMESTAMP,
                COL_TIMESTAMP + " AS " + UniversalQueueMessage.FIELD_TIMESTAMP,
                COL_NUM_REQUEUES + " AS " + UniversalQueueMessage.FIELD_NUM_REQUEUES,
                COL_CONTENT + " AS " + UniversalQueueMessage.FIELD_CONTENT);

        /*
         * Reads a message from ephemeral storage
         */
        SQL_READ_FROM_EPHEMERAL = "SELECT {1}, {2}, {3}, {4}, {5} FROM {0} WHERE " + COL_QUEUE_ID
                + "=?";
        SQL_READ_FROM_EPHEMERAL = MessageFormat.format(SQL_READ_FROM_EPHEMERAL,
                getTableNameEphemeral(),
                COL_QUEUE_ID + " AS " + UniversalQueueMessage.FIELD_QUEUE_ID,
                COL_ORG_TIMESTAMP + " AS " + UniversalQueueMessage.FIELD_ORG_TIMESTAMP,
                COL_TIMESTAMP + " AS " + UniversalQueueMessage.FIELD_TIMESTAMP,
                COL_NUM_REQUEUES + " AS " + UniversalQueueMessage.FIELD_NUM_REQUEUES,
                COL_CONTENT + " AS " + UniversalQueueMessage.FIELD_CONTENT);

        SQL_GET_ORPHAN_MSGS = "SELECT {1}, {2}, {3}, {4}, {5} FROM {0} WHERE " + COL_TIMESTAMP
                + "<?";
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

        SQL_PUT_TO_EPHEMERAL = "INSERT INTO {0} ({1}, {2}, {3}, {4}, {5}) VALUES (?, ?, ?, ?, ?)";
        SQL_PUT_TO_EPHEMERAL = MessageFormat.format(SQL_PUT_TO_EPHEMERAL, getTableNameEphemeral(),
                COL_QUEUE_ID, COL_ORG_TIMESTAMP, COL_TIMESTAMP, COL_NUM_REQUEUES, COL_CONTENT);

        SQL_REMOVE_FROM_QUEUE = "DELETE FROM {0} WHERE " + COL_QUEUE_ID + "=?";
        SQL_REMOVE_FROM_QUEUE = MessageFormat.format(SQL_REMOVE_FROM_QUEUE, getTableName());

        SQL_REMOVE_FROM_EPHEMERAL = "DELETE FROM {0} WHERE " + COL_QUEUE_ID + "=?";
        SQL_REMOVE_FROM_EPHEMERAL = MessageFormat.format(SQL_REMOVE_FROM_EPHEMERAL,
                getTableNameEphemeral());

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalQueueMessage readFromQueueStorage(Connection conn) {
        List<Map<String, Object>> dbRows = getJdbcHelper().executeSelect(conn, SQL_READ_FROM_QUEUE);
        if (dbRows != null && dbRows.size() > 0) {
            Map<String, Object> dbRow = dbRows.get(0);
            // ensureContentIsByteArray(dbRow);
            UniversalQueueMessage msg = new UniversalQueueMessage();
            return (UniversalQueueMessage) msg.fromMap(dbRow);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalQueueMessage readFromEphemeralStorage(Connection conn, IQueueMessage msg) {
        List<Map<String, Object>> dbRows = getJdbcHelper().executeSelect(conn,
                SQL_READ_FROM_EPHEMERAL, msg.qId());
        if (dbRows != null && dbRows.size() > 0) {
            Map<String, Object> dbRow = dbRows.get(0);
            // ensureContentIsByteArray(dbRow);
            UniversalQueueMessage myMsg = new UniversalQueueMessage();
            return (UniversalQueueMessage) myMsg.fromMap(dbRow);
        }
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
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }
        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        int numRows = getJdbcHelper().execute(conn, SQL_PUT_TO_EPHEMERAL, msg.qId(),
                msg.qOriginalTimestamp(), msg.qTimestamp(), msg.qNumRequeues(), msg.content());
        return numRows > 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean removeFromQueueStorage(Connection conn, IQueueMessage _msg) {
        if (!(_msg instanceof UniversalQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalQueueMessage.class.getName() + "]!");
        }
        UniversalQueueMessage msg = (UniversalQueueMessage) _msg;
        int numRows = getJdbcHelper().execute(conn, SQL_REMOVE_FROM_QUEUE, msg.qId());
        return numRows > 0;
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
    public UniversalQueueMessage take() {
        return (UniversalQueueMessage) super.take();
    }

}

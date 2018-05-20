package com.github.ddth.queue.impl.universal.idint;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.Map;
import java.util.stream.Stream;

import org.apache.commons.lang3.ArrayUtils;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.BaseUniversalJdbcQueue;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;

/**
 * Universal JDBC implementation of {@link IQueue}.
 * 
 * <p>
 * Queue and Take {@link UniversalIdIntQueueMessage}s.
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
public class UniversalJdbcQueue extends BaseUniversalJdbcQueue<UniversalIdIntQueueMessage, Long> {

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

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.0
     */
    @Override
    public UniversalIdIntQueueMessage createMessage() {
        return UniversalIdIntQueueMessage.newInstance();
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.0
     */
    @Override
    public UniversalIdIntQueueMessage createMessage(byte[] data) {
        return UniversalIdIntQueueMessage.newInstance(data);
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.0
     */
    @Override
    public UniversalIdIntQueueMessage createMessage(Long id, byte[] data) {
        return (UniversalIdIntQueueMessage) UniversalIdIntQueueMessage.newInstance(data).setId(id);
    }

    /*----------------------------------------------------------------------*/

    private String SQL_READ_FROM_QUEUE, SQL_READ_FROM_EPHEMERAL;
    private String SQL_GET_ORPHAN_MSGS;
    private String SQL_PUT_NEW_TO_QUEUE, SQL_REPUT_TO_QUEUE, SQL_PUT_TO_EPHEMERAL;
    private String SQL_REMOVE_FROM_QUEUE, SQL_REMOVE_FROM_EPHEMERAL;

    public UniversalJdbcQueue init() throws Exception {
        super.init();

        Object[] COLS_SELECT = { COL_QUEUE_ID + " AS " + UniversalIdIntQueueMessage.FIELD_QUEUE_ID,
                COL_ORG_TIMESTAMP + " AS " + UniversalIdIntQueueMessage.FIELD_TIMESTAMP,
                COL_TIMESTAMP + " AS " + UniversalIdIntQueueMessage.FIELD_QUEUE_TIMESTAMP,
                COL_NUM_REQUEUES + " AS " + UniversalIdIntQueueMessage.FIELD_NUM_REQUEUES,
                COL_CONTENT + " AS " + UniversalIdIntQueueMessage.FIELD_DATA };

        /*
         * Takes a message from queue
         */
        SQL_READ_FROM_QUEUE = "SELECT {1}, {2}, {3}, {4}, {5} FROM {0}"
                + (fifo ? (" ORDER BY " + COL_QUEUE_ID) : "");
        SQL_READ_FROM_QUEUE = MessageFormat.format(SQL_READ_FROM_QUEUE,
                ArrayUtils.insert(0, COLS_SELECT, getTableName()));

        /*
         * Reads a message from ephemeral storage
         */
        SQL_READ_FROM_EPHEMERAL = "SELECT {1}, {2}, {3}, {4}, {5} FROM {0} WHERE " + COL_QUEUE_ID
                + "=?";
        SQL_READ_FROM_EPHEMERAL = MessageFormat.format(SQL_READ_FROM_EPHEMERAL,
                ArrayUtils.insert(0, COLS_SELECT, getTableNameEphemeral()));

        SQL_GET_ORPHAN_MSGS = "SELECT {1}, {2}, {3}, {4}, {5} FROM {0} WHERE " + COL_TIMESTAMP
                + "<?";
        SQL_GET_ORPHAN_MSGS = MessageFormat.format(SQL_GET_ORPHAN_MSGS,
                ArrayUtils.insert(0, COLS_SELECT, getTableNameEphemeral()));

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
    protected UniversalIdIntQueueMessage readFromQueueStorage(Connection conn) {
        Map<String, Object> dbRow = getJdbcHelper().executeSelectOne(conn, SQL_READ_FROM_QUEUE);
        if (dbRow != null) {
            UniversalIdIntQueueMessage msg = new UniversalIdIntQueueMessage();
            return msg.fromMap(dbRow);
        }
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalIdIntQueueMessage readFromEphemeralStorage(Connection conn,
            IQueueMessage<Long, byte[]> msg) {
        Map<String, Object> dbRow = getJdbcHelper().executeSelectOne(conn, SQL_READ_FROM_EPHEMERAL,
                msg.getId());
        if (dbRow != null) {
            UniversalIdIntQueueMessage myMsg = new UniversalIdIntQueueMessage();
            return myMsg.fromMap(dbRow);
        }
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
                SQL_GET_ORPHAN_MSGS, threshold)) {
            dbRows.forEach(row -> {
                UniversalIdIntQueueMessage msg = new UniversalIdIntQueueMessage().fromMap(row);
                result.add(msg);
            });
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean putToQueueStorage(Connection conn, IQueueMessage<Long, byte[]> _msg) {
        if (!(_msg instanceof UniversalIdIntQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalIdIntQueueMessage.class.getName() + "]!");
        }
        UniversalIdIntQueueMessage msg = (UniversalIdIntQueueMessage) _msg;
        Long qid = msg.getId();
        if (qid == null || qid.longValue() == 0) {
            int numRows = getJdbcHelper().execute(conn, SQL_PUT_NEW_TO_QUEUE, msg.getTimestamp(),
                    msg.getQueueTimestamp(), msg.getNumRequeues(), msg.getContent());
            return numRows > 0;
        } else {
            int numRows = getJdbcHelper().execute(conn, SQL_REPUT_TO_QUEUE, qid, msg.getTimestamp(),
                    msg.getQueueTimestamp(), msg.getNumRequeues(), msg.getContent());
            return numRows > 0;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean putToEphemeralStorage(Connection conn, IQueueMessage<Long, byte[]> _msg) {
        if (!(_msg instanceof UniversalIdIntQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalIdIntQueueMessage.class.getName() + "]!");
        }
        UniversalIdIntQueueMessage msg = (UniversalIdIntQueueMessage) _msg;
        int numRows = getJdbcHelper().execute(conn, SQL_PUT_TO_EPHEMERAL, msg.getId(),
                msg.getTimestamp(), msg.getQueueTimestamp(), msg.getNumRequeues(),
                msg.getContent());
        return numRows > 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean removeFromQueueStorage(Connection conn, IQueueMessage<Long, byte[]> _msg) {
        if (!(_msg instanceof UniversalIdIntQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalIdIntQueueMessage.class.getName() + "]!");
        }
        UniversalIdIntQueueMessage msg = (UniversalIdIntQueueMessage) _msg;
        int numRows = getJdbcHelper().execute(conn, SQL_REMOVE_FROM_QUEUE, msg.getId());
        return numRows > 0;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean removeFromEphemeralStorage(Connection conn,
            IQueueMessage<Long, byte[]> _msg) {
        if (!(_msg instanceof UniversalIdIntQueueMessage)) {
            throw new IllegalArgumentException("This method requires an argument of type ["
                    + UniversalIdIntQueueMessage.class.getName() + "]!");
        }
        UniversalIdIntQueueMessage msg = (UniversalIdIntQueueMessage) _msg;
        int numRows = getJdbcHelper().execute(conn, SQL_REMOVE_FROM_EPHEMERAL, msg.getId());
        return numRows > 0;
    }

}

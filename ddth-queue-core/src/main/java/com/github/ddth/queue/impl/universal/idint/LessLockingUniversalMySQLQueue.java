package com.github.ddth.queue.impl.universal.idint;

import java.text.MessageFormat;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;

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
public class LessLockingUniversalMySQLQueue extends AbstractLessLockingUniversalJdbcQueue {

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
        return (UniversalIdIntQueueMessage) UniversalIdIntQueueMessage.newInstance(data).qId(id);
    }

    /**
     * {@inheritDoc}
     */
    @Override
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
                + (getFifo() ? (" ORDER BY " + COL_QUEUE_ID) : "") + " LIMIT 1";
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
                COL_QUEUE_ID + " AS " + UniversalIdIntQueueMessage.FIELD_QUEUE_ID,
                COL_ORG_TIMESTAMP + " AS " + UniversalIdIntQueueMessage.FIELD_ORG_TIMESTAMP,
                COL_TIMESTAMP + " AS " + UniversalIdIntQueueMessage.FIELD_TIMESTAMP,
                COL_NUM_REQUEUES + " AS " + UniversalIdIntQueueMessage.FIELD_NUM_REQUEUES,
                COL_CONTENT + " AS " + UniversalIdIntQueueMessage.FIELD_DATA, COL_EPHEMERAL_ID);

        SQL_GET_ORPHAN_MSGS = "SELECT {1}, {2}, {3}, {4}, {5} FROM {0} WHERE " + COL_EPHEMERAL_ID
                + "!=0 AND " + COL_TIMESTAMP + "<?";
        SQL_GET_ORPHAN_MSGS = MessageFormat.format(SQL_GET_ORPHAN_MSGS, getTableNameEphemeral(),
                COL_QUEUE_ID + " AS " + UniversalIdIntQueueMessage.FIELD_QUEUE_ID,
                COL_ORG_TIMESTAMP + " AS " + UniversalIdIntQueueMessage.FIELD_ORG_TIMESTAMP,
                COL_TIMESTAMP + " AS " + UniversalIdIntQueueMessage.FIELD_TIMESTAMP,
                COL_NUM_REQUEUES + " AS " + UniversalIdIntQueueMessage.FIELD_NUM_REQUEUES,
                COL_CONTENT + " AS " + UniversalIdIntQueueMessage.FIELD_DATA);

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

}

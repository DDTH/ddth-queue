package com.github.ddth.queue.impl.universal.idstr;

import java.text.MessageFormat;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdStrQueueMessage;

/**
 * PostgreSQL-specific implementation of
 * {@link AbstractLessLockingUniversalSingleStorageJdbcQueue}.
 *
 * <p>
 * Queue db table schema:
 * </p>
 * <ul>
 * <li>{@code queue_name}: {@code string}, queue's name, to group queue
 * messages</li>
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
 * @since 0.6.0
 */
public class LessLockingUniversalSingleStoragePgSQLQueue
        extends AbstractLessLockingUniversalSingleStorageJdbcQueue {

    public LessLockingUniversalSingleStoragePgSQLQueue init() throws Exception {
        super.init();

        final String WHERE_QUEUE_NAME = COL_QUEUE_NAME + "=?";
        final String WHERE_QUEUE_NAME_AND = WHERE_QUEUE_NAME + " AND ";

        /*
         * Sets column COL_EPHEMERAL_ID's value to nil, increases value of
         * column COL_NUM_REQUEUES, also updates value of column COL_TIMESTAMP
         */
        SQL_REQUEUE = "UPDATE {0} SET {1}=0,{2}={2}+1,{3}=? WHERE " + WHERE_QUEUE_NAME_AND
                + "{4}=?";
        SQL_REQUEUE = MessageFormat.format(SQL_REQUEUE, getTableName(), COL_EPHEMERAL_ID,
                COL_NUM_REQUEUES, COL_TIMESTAMP, COL_QUEUE_ID);

        /*
         * Sets column COL_EPHEMERAL_ID's value to nil
         */
        SQL_REQUEUE_SILENT = "UPDATE {0} SET {1}=0 WHERE " + WHERE_QUEUE_NAME_AND + "{2}=?";
        SQL_REQUEUE_SILENT = MessageFormat.format(SQL_REQUEUE_SILENT, getTableName(),
                COL_EPHEMERAL_ID, COL_QUEUE_ID);

        /*
         * Sets value of column COL_EPHEMERAL_ID for taking a queue message
         */
        SQL_UPDATE_EPHEMERAL_ID_TAKE = "UPDATE {0} M SET {1}=? FROM (SELECT {2},{3} FROM {0} WHERE "
                + WHERE_QUEUE_NAME_AND + "{1}=0" + (getFifo() ? (" ORDER BY {2}") : "")
                + " LIMIT 1 FOR UPDATE) S" + " WHERE M.{2}=S.{2} AND M.{3}=S.{3}";
        SQL_UPDATE_EPHEMERAL_ID_TAKE = MessageFormat.format(SQL_UPDATE_EPHEMERAL_ID_TAKE,
                getTableName(), COL_EPHEMERAL_ID, COL_QUEUE_ID, COL_QUEUE_NAME);

        /*
         * Sets column COL_EPHEMERAL_ID's value to nil (used in action
         * "move message from ephemeral storage to queue")
         */
        SQL_CLEAR_EPHEMERAL_ID = "UPDATE {0} SET {1}=0 WHERE " + WHERE_QUEUE_NAME_AND + "{2}=?";
        SQL_CLEAR_EPHEMERAL_ID = MessageFormat.format(SQL_CLEAR_EPHEMERAL_ID, getTableName(),
                COL_EPHEMERAL_ID, COL_QUEUE_ID);

        /*
         * Reads a queue message by ephemeral id
         */
        SQL_READ_BY_EPHEMERAL_ID = "SELECT {1},{2},{3},{4},{5} FROM {0} WHERE "
                + WHERE_QUEUE_NAME_AND + "{6}=?";
        SQL_READ_BY_EPHEMERAL_ID = MessageFormat.format(SQL_READ_BY_EPHEMERAL_ID, getTableName(),
                COL_QUEUE_ID + " AS " + UniversalIdStrQueueMessage.FIELD_QUEUE_ID,
                COL_ORG_TIMESTAMP + " AS " + UniversalIdStrQueueMessage.FIELD_TIMESTAMP,
                COL_TIMESTAMP + " AS " + UniversalIdStrQueueMessage.FIELD_QUEUE_TIMESTAMP,
                COL_NUM_REQUEUES + " AS " + UniversalIdStrQueueMessage.FIELD_NUM_REQUEUES,
                COL_CONTENT + " AS " + UniversalIdStrQueueMessage.FIELD_DATA, COL_EPHEMERAL_ID);

        SQL_GET_ORPHAN_MSGS = "SELECT {1},{2},{3},{4},{5} FROM {0} WHERE " + WHERE_QUEUE_NAME_AND
                + COL_EPHEMERAL_ID + "!=0 AND " + COL_TIMESTAMP + "<?";
        SQL_GET_ORPHAN_MSGS = MessageFormat.format(SQL_GET_ORPHAN_MSGS, getTableNameEphemeral(),
                COL_QUEUE_ID + " AS " + UniversalIdStrQueueMessage.FIELD_QUEUE_ID,
                COL_ORG_TIMESTAMP + " AS " + UniversalIdStrQueueMessage.FIELD_TIMESTAMP,
                COL_TIMESTAMP + " AS " + UniversalIdStrQueueMessage.FIELD_QUEUE_TIMESTAMP,
                COL_NUM_REQUEUES + " AS " + UniversalIdStrQueueMessage.FIELD_NUM_REQUEUES,
                COL_CONTENT + " AS " + UniversalIdStrQueueMessage.FIELD_DATA);

        /*
         * Puts a new message (message without pre-set queue id) to queue,
         * assuming column COL_QUEUE_ID is auto-number
         */
        SQL_PUT_NEW_TO_QUEUE = "INSERT INTO {0} ({1},{2},{3},{4},{5}) VALUES (?,?,?,?,?)";
        SQL_PUT_NEW_TO_QUEUE = MessageFormat.format(SQL_PUT_NEW_TO_QUEUE, getTableName(),
                COL_QUEUE_NAME, COL_ORG_TIMESTAMP, COL_TIMESTAMP, COL_NUM_REQUEUES, COL_CONTENT);

        /*
         * Put a message with pre-set queue id to queue
         */
        SQL_REPUT_TO_QUEUE = "INSERT INTO {0} ({1},{2},{3},{4},{5},{6}) VALUES (?,?,?,?,?,?)";
        SQL_REPUT_TO_QUEUE = MessageFormat.format(SQL_REPUT_TO_QUEUE, getTableName(),
                COL_QUEUE_NAME, COL_QUEUE_ID, COL_ORG_TIMESTAMP, COL_TIMESTAMP, COL_NUM_REQUEUES,
                COL_CONTENT);

        SQL_REMOVE_FROM_EPHEMERAL = "DELETE FROM {0} WHERE " + WHERE_QUEUE_NAME_AND + COL_QUEUE_ID
                + "=?";
        SQL_REMOVE_FROM_EPHEMERAL = MessageFormat.format(SQL_REMOVE_FROM_EPHEMERAL,
                getTableNameEphemeral());

        return this;
    }

}

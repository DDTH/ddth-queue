package com.github.ddth.queue.impl.universal.idint;

import java.sql.Connection;
import java.text.MessageFormat;
import java.util.Map;

import com.github.ddth.commons.utils.MapUtils;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.utils.DefaultNamedParamsFilters;
import com.github.ddth.dao.jdbc.utils.DefaultNamedParamsSqlBuilders;
import com.github.ddth.dao.utils.DaoException;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.internal.utils.QueueUtils;

/**
 * PostgreSQL-specific implementation of {@link AbstractLessLockingUniversalSingleStorageJdbcQueue}.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @see AbstractLessLockingUniversalSingleStorageJdbcQueue
 * @since 0.6.0
 */
public class LessLockingUniversalSingleStoragePgSQLQueue extends AbstractLessLockingUniversalSingleStorageJdbcQueue {
    private String SQL_UPDATE_EPHEMERAL_ID_TAKE, SQL_GET_MSG_BY_EPHEPERAL_ID;

    /**
     * {@inheritDoc}
     */
    @Override
    public LessLockingUniversalSingleStoragePgSQLQueue init() throws Exception {
        String[] COLUMNS_SELECT = { COL_QUEUE_ID + " AS " + UniversalIdIntQueueMessage.FIELD_QUEUE_ID,
                COL_ORG_TIMESTAMP + " AS " + UniversalIdIntQueueMessage.FIELD_TIMESTAMP,
                COL_TIMESTAMP + " AS " + UniversalIdIntQueueMessage.FIELD_QUEUE_TIMESTAMP,
                COL_NUM_REQUEUES + " AS " + UniversalIdIntQueueMessage.FIELD_NUM_REQUEUES,
                COL_CONTENT + " AS " + UniversalIdIntQueueMessage.FIELD_DATA };

        /* update value of column COL_EPHEMERAL_ID for taking a queue message off */
        SQL_UPDATE_EPHEMERAL_ID_TAKE = MessageFormat
                .format("UPDATE {0} M SET {1}=? FROM (SELECT {2} FROM {0} WHERE {4}=? AND {1}=0" + (getFifo() ?
                                (" ORDER BY {3}") :
                                "") + " LIMIT 1 FOR UPDATE) S WHERE M.{2}=S.{2}", getTableName(), COL_EPHEMERAL_ID,
                        COL_QUEUE_ID, COL_ORG_TIMESTAMP, COL_QUEUE_NAME);
        /* get a queue message by COL_EPHEMERAL_ID */
        SQL_GET_MSG_BY_EPHEPERAL_ID = new DefaultNamedParamsSqlBuilders.SelectBuilder().withColumns(COLUMNS_SELECT)
                .withFilterWhere(new DefaultNamedParamsFilters.FilterAnd()
                        .addFilter(new DefaultNamedParamsFilters.FilterFieldValue(COL_QUEUE_NAME, "=", "dummy"))
                        .addFilter(new DefaultNamedParamsFilters.FilterFieldValue(COL_EPHEMERAL_ID, "=", "dummy")))
                .withTableNames(getTableNameEphemeral()).build().clause;

        super.init();
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Implementation:</p>
     * <ul>
     * <li>Generate a unique-id and assign to the message's {@link #COL_EPHEMERAL_ID} using {@code UPDATE...FROM (SELECT...ORDER BY...LIMIT 1 FOR UPDATE)}</li>
     * <li>Get the queue message that has been assigned the ephemeral-id and return it</li>
     * </ul>
     */
    @Override
    protected UniversalIdIntQueueMessage _takeWithRetries(Connection conn, int numRetries, int maxRetries) {
        IJdbcHelper jdbcHelper = getJdbcHelper();
        return executeWithRetries(numRetries, maxRetries, () -> {
            try {
                long ephemeralId = QueueUtils.IDGEN.generateId64();
                int numRows = jdbcHelper.execute(conn, SQL_UPDATE_EPHEMERAL_ID_TAKE, ephemeralId, getQueueName());
                if (numRows > 0) {
                    Map<String, Object> dbRow = jdbcHelper.executeSelectOne(conn, SQL_GET_MSG_BY_EPHEPERAL_ID,
                            MapUtils.createMap(COL_QUEUE_NAME, getQueueName(), COL_EPHEMERAL_ID, ephemeralId));
                    return dbRow != null ? createMessge(dbRow) : null;
                }
                return null;
            } catch (Exception e) {
                jdbcHelper.rollbackTransaction(conn);
                throw e instanceof DaoException ? (DaoException) e : new DaoException(e);
            }
        });
    }
}

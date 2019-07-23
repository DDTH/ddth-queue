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
 * Same as {@link UniversalJdbcQueue}, but using a "less-locking" algorithm - specific for MySQL,
 * requires only one single db table for both queue and ephemeral storage.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @see AbstractLessLockingUniversalJdbcQueue
 * @since 0.2.3
 */
public class LessLockingUniversalMySQLQueue extends AbstractLessLockingUniversalJdbcQueue {
    private String SQL_UPDATE_EPHEMERAL_ID_TAKE, SQL_GET_MSG_BY_EPHEPERAL_ID;

    /**
     * {@inheritDoc}
     */
    @Override
    public LessLockingUniversalMySQLQueue init() throws Exception {
        String[] COLUMNS_SELECT = { COL_QUEUE_ID + " AS " + UniversalIdIntQueueMessage.FIELD_QUEUE_ID,
                COL_ORG_TIMESTAMP + " AS " + UniversalIdIntQueueMessage.FIELD_TIMESTAMP,
                COL_TIMESTAMP + " AS " + UniversalIdIntQueueMessage.FIELD_QUEUE_TIMESTAMP,
                COL_NUM_REQUEUES + " AS " + UniversalIdIntQueueMessage.FIELD_NUM_REQUEUES,
                COL_CONTENT + " AS " + UniversalIdIntQueueMessage.FIELD_DATA };

        /* update value of column COL_EPHEMERAL_ID for taking a queue message off */
        SQL_UPDATE_EPHEMERAL_ID_TAKE = MessageFormat
                .format("UPDATE {0} SET {1}=? WHERE {1}=0" + (getFifo() ? (" ORDER BY " + COL_ORG_TIMESTAMP) : "")
                        + " LIMIT 1", getTableName(), COL_EPHEMERAL_ID);
        /* get a queue message by COL_EPHEMERAL_ID */
        SQL_GET_MSG_BY_EPHEPERAL_ID = new DefaultNamedParamsSqlBuilders.SelectBuilder().withColumns(COLUMNS_SELECT)
                .withFilterWhere(new DefaultNamedParamsFilters.FilterFieldValue(COL_EPHEMERAL_ID, "=", "dummy"))
                .withTableNames(getTableNameEphemeral()).build().clause;

        super.init();
        return this;
    }

    /**
     * {@inheritDoc}
     *
     * <p>Implementation:</p>
     * <ul>
     * <li>Generate a unique-id and assign to the message's {@link #COL_EPHEMERAL_ID} using {@code UPDATE...ORDER BY...LIMIT 1}</li>
     * <li>Get the queue message that has been assigned the ephemeral-id and return it</li>
     * </ul>
     */
    @Override
    protected UniversalIdIntQueueMessage _takeWithRetries(Connection conn, int numRetries, int maxRetries) {
        IJdbcHelper jdbcHelper = getJdbcHelper();
        return executeWithRetries(numRetries, maxRetries, () -> {
            try {
                long ephemeralId = QueueUtils.IDGEN.generateId64();
                int numRows = jdbcHelper.execute(conn, SQL_UPDATE_EPHEMERAL_ID_TAKE, ephemeralId);
                if (numRows > 0) {
                    Map<String, Object> dbRow = jdbcHelper.executeSelectOne(conn, SQL_GET_MSG_BY_EPHEPERAL_ID,
                            MapUtils.createMap(COL_EPHEMERAL_ID, ephemeralId));
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

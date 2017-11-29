package com.github.ddth.queue.impl;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.queue.QueueSpec;

/**
 * Factory to create {@link JdbcQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public abstract class JdbcQueueFactory<T extends JdbcQueue<ID, DATA>, ID, DATA>
        extends AbstractQueueFactory<T, ID, DATA> {

    public final static String SPEC_FIELD_TABLE_NAME = "table_name";
    public final static String SPEC_FIELD_TABLE_NAME_EPHEMERAL = "table_name_ephemeral";
    public final static String SPEC_FIELD_MAX_RETRIES = "max_retries";
    public final static String SPEC_FIELD_TRANSACTION_ISOLATION_LEVEL = "tranx_isolation_level";

    private IJdbcHelper defaultJdbcHelper;

    /**
     * 
     * @return
     * @since 0.5.1.1
     */
    public IJdbcHelper getDefaultJdbcHeper() {
        return defaultJdbcHelper;
    }

    /**
     * 
     * @param jdbcHelper
     * @return
     * @since 0.5.1.1
     */
    public JdbcQueueFactory<T, ID, DATA> setDefaultJdbcHeper(IJdbcHelper jdbcHelper) {
        this.defaultJdbcHelper = jdbcHelper;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void initQueue(T queue, QueueSpec spec) {
        queue.setJdbcHelper(defaultJdbcHelper);

        Boolean ephemeralDisabled = spec.getField(QueueSpec.FIELD_EPHEMERAL_DISABLED,
                Boolean.class);
        if (ephemeralDisabled != null) {
            queue.setEphemeralDisabled(ephemeralDisabled.booleanValue());
        }
        Integer maxEphemeralSize = spec.getField(QueueSpec.FIELD_EPHEMERAL_MAX_SIZE, Integer.class);
        if (maxEphemeralSize != null) {
            queue.setEphemeralMaxSize(maxEphemeralSize.intValue());
        }

        String tableName = spec.getField(SPEC_FIELD_TABLE_NAME);
        if (!StringUtils.isBlank(tableName)) {
            queue.setTableName(tableName);
        }

        String tableNameEphemeral = spec.getField(SPEC_FIELD_TABLE_NAME_EPHEMERAL);
        if (!StringUtils.isBlank(tableNameEphemeral)) {
            queue.setTableNameEphemeral(tableNameEphemeral);
        }

        Integer maxRetries = spec.getField(SPEC_FIELD_MAX_RETRIES, Integer.class);
        if (maxRetries != null) {
            queue.setMaxRetries(maxRetries.intValue());
        }

        Integer txIsolationLevel = spec.getField(SPEC_FIELD_TRANSACTION_ISOLATION_LEVEL,
                Integer.class);
        if (txIsolationLevel != null) {
            queue.setTransactionIsolationLevel(txIsolationLevel.intValue());
        }

        queue.init();
    }

}

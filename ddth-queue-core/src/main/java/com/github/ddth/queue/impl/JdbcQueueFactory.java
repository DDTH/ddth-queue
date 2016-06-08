package com.github.ddth.queue.impl;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.queue.QueueSpec;

/**
 * Factory to create {@link JdbcQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public abstract class JdbcQueueFactory<T extends JdbcQueue> extends AbstractQueueFactory<T> {

    public final static String SPEC_FIELD_TABLE_NAME = "table_name";
    public final static String SPEC_FIELD_TABLE_NAME_EPHEMERAL = "table_name_ephemeral";
    public final static String SPEC_FIELD_MAX_RETRIES = "max_retries";
    public final static String SPEC_FIELD_TRANSACTION_ISOLATION_LEVEL = "tranx_isolation_level";

    private DataSource defaultDataSource;

    public DataSource getDefaultDataSource() {
        return defaultDataSource;
    }

    public JdbcQueueFactory<T> setDefaultDataSource(DataSource dataSource) {
        this.defaultDataSource = dataSource;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void initQueue(T queue, QueueSpec spec) {
        queue.setDataSource(getDefaultDataSource());

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

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean disposeQueue(T queue) {
        queue.destroy();
        return true;
    }

}

package com.github.ddth.queue.impl;

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
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

    private final Logger LOGGER = LoggerFactory.getLogger(JdbcQueueFactory.class);

    public final static String SPEC_FIELD_TABLE_NAME = "table_name";
    public final static String SPEC_FIELD_TABLE_NAME_EPHEMERAL = "table_name_ephemeral";
    public final static String SPEC_FIELD_MAX_RETRIES = "max_retries";
    public final static String SPEC_FIELD_TRANSACTION_ISOLATION_LEVEL = "tranx_isolation_level";

    private DataSource defaultDataSource;
    private IJdbcHelper defaultJdbcHelper;
    private boolean myOwnJdbcHelper = false;
    private String defaultTableName, defaultTableNameEphemeral;
    private int defaultMaxRetries = JdbcQueue.DEFAULT_MAX_RETRIES;
    private int defaultTransactionIsolationLevel = JdbcQueue.DEFAULT_TRANX_ISOLATION_LEVEL;

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultTableName() {
        return defaultTableName;
    }

    /**
     * 
     * @param defaultTableName
     * @since 0.6.2
     */
    public JdbcQueueFactory<T, ID, DATA> setDefaultTableName(String defaultTableName) {
        this.defaultTableName = defaultTableName;
        return this;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public String getDefaultTableNameEphemeral() {
        return defaultTableNameEphemeral;
    }

    /**
     * 
     * @param defaultTableNameEphemeral
     * @since 0.6.2
     */
    public JdbcQueueFactory<T, ID, DATA> setDefaultTableNameEphemeral(
            String defaultTableNameEphemeral) {
        this.defaultTableNameEphemeral = defaultTableNameEphemeral;
        return this;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public int getDefaultMaxRetries() {
        return defaultMaxRetries;
    }

    /**
     * 
     * @param defaultMaxRetries
     * @since 0.6.2
     */
    public JdbcQueueFactory<T, ID, DATA> setDefaultMaxRetries(int defaultMaxRetries) {
        this.defaultMaxRetries = defaultMaxRetries;
        return this;
    }

    /**
     * 
     * @return
     * @since 0.6.2
     */
    public int getDefaultTransactionIsolationLevel() {
        return defaultTransactionIsolationLevel;
    }

    public JdbcQueueFactory<T, ID, DATA> setDefaultTransactionIsolationLevel(
            int defaultTransactionIsolationLevel) {
        this.defaultTransactionIsolationLevel = defaultTransactionIsolationLevel;
        return this;
    }

    /**
     * Getter for {@link #defaultJdbcHelper}.
     * 
     * <p>
     * If all {@link JdbcQueue} instances are connecting to one
     * {@link DataSource}, it's a good idea to pre-create a {@link IJdbcHelper}
     * instance and share it amongst {@link JdbcQueue} instances created from
     * this factory by assigning it to {@link #defaultJdbcHelper} (see
     * {@link #setDefaultJdbcHelper(IJdbcHelper)}).
     * </p>
     * 
     * @return
     * @since 0.5.1.1
     */
    protected IJdbcHelper getDefaultJdbcHelper() {
        return defaultJdbcHelper;
    }

    /**
     * Setter for {@link #defaultJdbcHelper}.
     * 
     * @param jdbcHelper
     * @param setMyOwnJdbcHelper
     * @return
     * @since 0.7.1
     */
    protected JdbcQueueFactory<T, ID, DATA> setDefaultJdbcHelper(IJdbcHelper jdbcHelper,
            boolean setMyOwnJdbcHelper) {
        if (myOwnJdbcHelper && this.defaultJdbcHelper != null
                && this.defaultJdbcHelper instanceof AbstractJdbcHelper) {
            ((AbstractJdbcHelper) this.defaultJdbcHelper).destroy();
        }
        this.defaultJdbcHelper = jdbcHelper;
        this.myOwnJdbcHelper = setMyOwnJdbcHelper;
        return this;
    }

    /**
     * Setter for {@link #defaultJdbcHelper}.
     * 
     * @param jdbcHelper
     * @return
     * @since 0.5.1.1
     */
    public JdbcQueueFactory<T, ID, DATA> setDefaultJdbcHelper(IJdbcHelper jdbcHelper) {
        return setDefaultJdbcHelper(jdbcHelper, false);
    }

    /**
     * 
     * @return
     * @since 0.6.2.6
     */
    public DataSource getDefaultDataSource() {
        return defaultDataSource;
    }

    /**
     * 
     * @param dataSource
     * @return
     * @since 0.6.2.6
     */
    public JdbcQueueFactory<T, ID, DATA> setDefaultDataSource(DataSource dataSource) {
        this.defaultDataSource = dataSource;
        return this;
    }

    /**
     * {@inheritDoc}
     * 
     * @since 0.6.2.6
     */
    @Override
    public void destroy() {
        try {
            super.destroy();
        } finally {
            if (myOwnJdbcHelper && defaultJdbcHelper != null
                    && defaultJdbcHelper instanceof AbstractJdbcHelper) {
                try {
                    ((AbstractJdbcHelper) defaultJdbcHelper).destroy();
                } catch (Exception e) {
                    LOGGER.warn(e.getMessage(), e);
                } finally {
                    defaultJdbcHelper = null;
                }
            }
        }
    }

    /**
     * {@inheritDoc}
     * 
     * @throws Exception
     */
    @Override
    protected void initQueue(T queue, QueueSpec spec) throws Exception {
        queue.setJdbcHelper(defaultJdbcHelper).setDataSource(defaultDataSource);

        queue.setEphemeralDisabled(getDefaultEphemeralDisabled())
                .setEphemeralMaxSize(getDefaultEphemeralMaxSize());
        Boolean ephemeralDisabled = spec.getField(QueueSpec.FIELD_EPHEMERAL_DISABLED,
                Boolean.class);
        if (ephemeralDisabled != null) {
            queue.setEphemeralDisabled(ephemeralDisabled.booleanValue());
        }
        Integer maxEphemeralSize = spec.getField(QueueSpec.FIELD_EPHEMERAL_MAX_SIZE, Integer.class);
        if (maxEphemeralSize != null) {
            queue.setEphemeralMaxSize(maxEphemeralSize.intValue());
        }

        queue.setTableName(defaultTableName).setTableNameEphemeral(defaultTableNameEphemeral)
                .setMaxRetries(defaultMaxRetries)
                .setTransactionIsolationLevel(defaultTransactionIsolationLevel);
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

        super.initQueue(queue, spec);
    }

}

package com.github.ddth.queue.test.universal.idint.mysql;

import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.NoopQueueObserver;
import com.github.ddth.queue.impl.JdbcQueue;
import com.github.ddth.queue.impl.universal.idint.LessLockingUniversalMySQLQueue;
import com.github.ddth.queue.test.universal.BaseQueueFunctionalTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.mysql.TestMySQLLLQueue -DenableTestsMySql=true
 */

/**
 * Test queue functionality.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.5.0
 */
public class TestMySQLLLQueue extends BaseQueueFunctionalTest<Long> {
    public TestMySQLLLQueue(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestMySQLLLQueue.class);
    }

    private static class MyJdbcQueue extends LessLockingUniversalMySQLQueue {
        public void flush() throws SQLException {
            IJdbcHelper jdbcHelper = getJdbcHelper();
            jdbcHelper.execute("DELETE FROM " + getTableName());
        }
    }

    protected IQueue<Long, byte[]> initQueueInstance(int ephemeralMaxSize) throws Exception {
        if (System.getProperty("enableTestsMySql") == null
                && System.getProperty("enableTestsMySQL") == null) {
            return null;
        }
        String mysqlHost = System.getProperty("db.host", "localhost");
        String mysqlPort = System.getProperty("db.port", "3306");
        String mysqlDb = System.getProperty("db.db", "test");
        String mysqlUser = System.getProperty("db.user", "test");
        String mysqlPassword = System.getProperty("db.password", "test");
        String tableQueue = System.getProperty("table.queue", "queuell");

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://" + mysqlHost + ":" + mysqlPort + "/" + mysqlDb
                + "?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8");
        dataSource.setUsername(mysqlUser);
        dataSource.setPassword(mysqlPassword);

        AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper();
        try {
            jdbcHelper.setDataSource(dataSource).init();
            MyJdbcQueue queue = new MyJdbcQueue();
            queue.setObserver(new NoopQueueObserver<Long, byte[]>() {
                public void postDestroy(IQueue<Long, byte[]> queue) {
                    if (queue instanceof JdbcQueue) {
                        IJdbcHelper jdbcHelper = ((JdbcQueue<?, ?>) queue).getJdbcHelper();
                        if (jdbcHelper instanceof AbstractJdbcHelper) {
                            ((AbstractJdbcHelper) jdbcHelper).destroy();
                        }
                    }
                }
            });
            queue.setJdbcHelper(jdbcHelper).setTableName(tableQueue).setEphemeralDisabled(false)
                    .setEphemeralMaxSize(ephemeralMaxSize)
                    .setQueueName(this.getClass().getSimpleName()).init();
            queue.flush();
            return queue;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}

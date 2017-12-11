package com.github.ddth.queue.test.universal.idstr.mysql;

import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.NoopQueueObserver;
import com.github.ddth.queue.impl.JdbcQueue;
import com.github.ddth.queue.impl.universal.idstr.UniversalJdbcQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.mysql.TestMySQLQueueMT -DenableTestsMySql=true
 */

public class TestMySQLQueueMT extends BaseQueueMultiThreadsTest<String> {
    public TestMySQLQueueMT(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestMySQLQueueMT.class);
    }

    private static class MyJdbcQueue extends UniversalJdbcQueue {
        public void flush() throws SQLException {
            IJdbcHelper jdbcHelper = getJdbcHelper();
            jdbcHelper.execute("DELETE FROM " + getTableName());
            jdbcHelper.execute("DELETE FROM " + getTableNameEphemeral());
        }
    }

    protected IQueue<String, byte[]> initQueueInstance() {
        if (System.getProperty("enableTestsMySql") == null
                && System.getProperty("enableTestsMySQL") == null) {
            return null;
        }
        String mysqlHost = System.getProperty("db.host", "localhost");
        String mysqlPort = System.getProperty("db.port", "3306");
        String mysqlDb = System.getProperty("db.db", "test");
        String mysqlUser = System.getProperty("db.user", "test");
        String mysqlPassword = System.getProperty("db.password", "test");
        String tableQueue = System.getProperty("table.queue", "queue2");
        String tableEphemeral = System.getProperty("table.ephemeral", "queue2_ephemeral");

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
            queue.setObserver(new NoopQueueObserver<String, byte[]>() {
                public void postDestroy(IQueue<String, byte[]> queue) {
                    if (queue instanceof JdbcQueue) {
                        IJdbcHelper jdbcHelper = ((JdbcQueue<?, ?>) queue).getJdbcHelper();
                        if (jdbcHelper instanceof AbstractJdbcHelper) {
                            ((AbstractJdbcHelper) jdbcHelper).destroy();
                        }
                    }
                }
            });
            queue.setJdbcHelper(jdbcHelper).setTableName(tableQueue)
                    .setTableNameEphemeral(tableEphemeral).setEphemeralDisabled(false).init();
            queue.flush();
            return queue;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected int numTestMessages() {
        return 4 * 1024;
    }

}

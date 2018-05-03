package com.github.ddth.queue.test.universal.idstr.mysql;

import java.sql.SQLException;

import org.apache.commons.dbcp2.BasicDataSource;

import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.NoopQueueObserver;
import com.github.ddth.queue.impl.universal.idstr.UniversalJdbcQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;

import junit.framework.Test;
import junit.framework.TestSuite;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.mysql.TestMySQLQueueMTBoundEphemeralSize -DenableTestsMySql=true
 */

public class TestMySQLQueueMTBoundEphemeralSize extends BaseQueueMultiThreadsTest<String> {
    public TestMySQLQueueMTBoundEphemeralSize(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestMySQLQueueMTBoundEphemeralSize.class);
    }

    private static class MyJdbcQueue extends UniversalJdbcQueue {
        public void flush() throws SQLException {
            IJdbcHelper jdbcHelper = getJdbcHelper();
            jdbcHelper.execute("DELETE FROM " + getTableName());
            jdbcHelper.execute("DELETE FROM " + getTableNameEphemeral());
        }
    }

    protected IQueue<String, byte[]> initQueueInstance() throws Exception {
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

        MyJdbcQueue queue = new MyJdbcQueue();
        queue.setObserver(new NoopQueueObserver<String, byte[]>() {
            public void postDestroy(IQueue<String, byte[]> queue) {
                try {
                    dataSource.close();
                } catch (SQLException e) {
                    e.printStackTrace();
                }
            }
        });
        queue.setDataSource(dataSource).setTableName(tableQueue)
                .setTableNameEphemeral(tableEphemeral).setEphemeralDisabled(false)
                .setEphemeralMaxSize(16).init();
        queue.flush();

        return queue;
    }

    protected int numTestMessages() {
        return 4 * 1024;
    }

}

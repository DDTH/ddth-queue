package com.github.ddth.queue.test.universal.idstr.mysql;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueMultiThreadsTest;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.SQLException;

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

    protected IQueue<String, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsMySql") == null && System.getProperty("enableTestsMySQL") == null) {
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

        MyQueue queue = new MyQueue() {
            public void destroy() {
                try {
                    super.destroy();
                } finally {
                    try {
                        dataSource.close();
                    } catch (SQLException e) {
                        e.printStackTrace();
                    }
                }
            }
        };
        queue.setDataSource(dataSource).setTableName(tableQueue).setTableNameEphemeral(tableEphemeral)
                .setEphemeralDisabled(false).init();
        queue.flush();

        return queue;
    }

    protected int numTestMessages() {
        return 1 * 1024;
    }
}

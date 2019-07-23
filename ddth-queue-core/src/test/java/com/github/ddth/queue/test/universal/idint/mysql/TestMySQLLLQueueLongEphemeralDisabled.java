package com.github.ddth.queue.test.universal.idint.mysql;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueLongTest;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.SQLException;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.mysql.TestMySQLLLQueueLongEphemeralDisabled -DenableTestsMySql=true
 */

public class TestMySQLLLQueueLongEphemeralDisabled extends BaseQueueLongTest<Long> {
    public TestMySQLLLQueueLongEphemeralDisabled(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestMySQLLLQueueLongEphemeralDisabled.class);
    }

    protected IQueue<Long, byte[]> initQueueInstance() throws Exception {
        if (System.getProperty("enableTestsMySql") == null && System.getProperty("enableTestsMySQL") == null) {
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

        MyLLQueue queue = new MyLLQueue() {
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
        queue.setDataSource(dataSource).setTableName(tableQueue).setEphemeralDisabled(true).init();
        queue.flush();

        return queue;
    }

    protected int numTestMessages() {
        // to make a very long queue
        return 1 * 1024;
    }
}

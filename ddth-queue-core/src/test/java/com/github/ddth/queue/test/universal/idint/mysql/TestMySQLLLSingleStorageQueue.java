package com.github.ddth.queue.test.universal.idint.mysql;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.test.universal.BaseQueueFunctionalTest;
import junit.framework.Test;
import junit.framework.TestSuite;
import org.apache.commons.dbcp2.BasicDataSource;

import java.sql.SQLException;

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.mysql.TestMySQLLLSingleStorageQueue -DenableTestsMySql=true
 */

/**
 * Test queue functionality.
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.6.0
 */
public class TestMySQLLLSingleStorageQueue extends BaseQueueFunctionalTest<Long> {
    public TestMySQLLLSingleStorageQueue(String testName) {
        super(testName);
    }

    public static Test suite() {
        return new TestSuite(TestMySQLLLSingleStorageQueue.class);
    }

    protected IQueue<Long, byte[]> initQueueInstance(int ephemeralMaxSize) throws Exception {
        if (System.getProperty("enableTestsMySql") == null && System.getProperty("enableTestsMySQL") == null) {
            return null;
        }
        String mysqlHost = System.getProperty("db.host", "localhost");
        String mysqlPort = System.getProperty("db.port", "3306");
        String mysqlDb = System.getProperty("db.db", "test");
        String mysqlUser = System.getProperty("db.user", "test");
        String mysqlPassword = System.getProperty("db.password", "test");
        String tableQueue = System.getProperty("table.queue", "queuellss");

        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl("jdbc:mysql://" + mysqlHost + ":" + mysqlPort + "/" + mysqlDb
                + "?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8");
        dataSource.setUsername(mysqlUser);
        dataSource.setPassword(mysqlPassword);

        MyLLSSQueue queue = new MyLLSSQueue() {
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
        queue.setDataSource(dataSource).setTableName(tableQueue).setEphemeralDisabled(false)
                .setEphemeralMaxSize(ephemeralMaxSize).setQueueName(this.getClass().getSimpleName()).init();
        queue.flush();

        return queue;
    }
}

package com.github.ddth.queue.qnd.universal;

import java.util.Date;

import org.apache.commons.dbcp2.BasicDataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.idint.LessLockingUniversalMySQLQueue;

public class QndLessLockingQueueMySQL {
    public static void main(String[] args) throws Exception {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource.setUrl(
                "jdbc:mysql://localhost:3306/test?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8");
        dataSource.setUsername("test");
        dataSource.setPassword("test");

        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            jdbcHelper.setDataSource(dataSource).init();

            try (LessLockingUniversalMySQLQueue queue = new LessLockingUniversalMySQLQueue()) {
                queue.setTableName("queuell").setJdbcHelper(jdbcHelper).init();

                UniversalIdIntQueueMessage msg = UniversalIdIntQueueMessage.newInstance();
                msg.setContent("Content: [" + System.currentTimeMillis() + "] " + new Date());
                System.out.println("Queue: " + queue.queue(msg));

                msg = queue.take();
                while (msg.getNumRequeues() < 2) {
                    System.out.println("Message: " + msg);
                    System.out.println("Content: " + new String(msg.getContent()));
                    System.out.println("Requeue: " + queue.requeue(msg));
                    msg = queue.take();
                }

                queue.finish(msg);
            }
        }
    }
}

package com.github.ddth.queue.qnd.universal;

import java.util.Date;

import org.apache.commons.dbcp2.BasicDataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.idint.LessLockingUniversalPgSQLQueue;

public class QndLessLockingQueuePgSQL {
    public static void main(String[] args) throws Exception {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(
                "jdbc:postgresql://localhost:5432/temp?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8");
        dataSource.setUsername("test");
        dataSource.setPassword("test");

        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            jdbcHelper.setDataSource(dataSource).init();

            try (LessLockingUniversalPgSQLQueue queue = new LessLockingUniversalPgSQLQueue()) {
                queue.setTableName("queuell").setJdbcHelper(jdbcHelper).init();

                UniversalIdIntQueueMessage msg = UniversalIdIntQueueMessage.newInstance();
                msg.setContent("Content: [" + System.currentTimeMillis() + "] " + new Date());
                System.out.println("Queue: " + queue.queue(msg));

                msg = queue.take();
                while (msg.getNumRequeues() < 2) {
                    System.out.println(msg);
                    System.out.println("Requeue: " + queue.requeue(msg));
                    msg = queue.take();
                }

                queue.finish(msg);
            }
        }
    }
}

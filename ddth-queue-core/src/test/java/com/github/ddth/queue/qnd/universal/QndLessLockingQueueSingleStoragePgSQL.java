package com.github.ddth.queue.qnd.universal;

import java.util.Date;

import org.apache.commons.dbcp2.BasicDataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;
import com.github.ddth.queue.impl.universal.idint.LessLockingUniversalSingleStoragePgSQLQueue;
import com.github.ddth.queue.impl.universal.msg.UniversalIdIntQueueMessage;

public class QndLessLockingQueueSingleStoragePgSQL {

    public static void main(String[] args) throws Exception {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl("jdbc:postgresql://localhost:5432/temp");
        dataSource.setUsername("test");
        dataSource.setPassword("test");

        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            jdbcHelper.setDataSource(dataSource).init();

            try (LessLockingUniversalSingleStoragePgSQLQueue queue = new LessLockingUniversalSingleStoragePgSQLQueue()) {
                queue.setTableName("queuellss").setJdbcHelper(jdbcHelper)
                        .setQueueName(QndLessLockingQueueSingleStoragePgSQL.class.getSimpleName()).init();

                UniversalIdIntQueueMessage msg = UniversalIdIntQueueMessage.newInstance();
                msg.content("Content: [" + System.currentTimeMillis() + "] " + new Date());
                System.out.println("Queue: " + queue.queue(msg));

                msg = queue.take();
                while (msg.getNumRequeues() < 2) {
                    System.out.println("Message: " + msg);
                    System.out.println("Content: " + new String(msg.content()));
                    System.out.println("Requeue: " + queue.requeue(msg));
                    msg = queue.take();
                }

                queue.finish(msg);
            }
        }
    }
}

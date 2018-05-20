package com.github.ddth.queue.qnd.universal;

import java.util.Date;

import org.apache.commons.dbcp2.BasicDataSource;

import com.github.ddth.dao.jdbc.AbstractJdbcHelper;
import com.github.ddth.dao.jdbc.impl.DdthJdbcHelper;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalSingleStorageJdbcQueue;

public class QndQueueSingleStoragePgSQL {

    public static void main(String[] args) throws Exception {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource.setUrl(
                "jdbc:postgresql://localhost:5432/temp?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8");
        dataSource.setUsername("test");
        dataSource.setPassword("test");

        try (AbstractJdbcHelper jdbcHelper = new DdthJdbcHelper()) {
            jdbcHelper.setDataSource(dataSource).init();

            try (UniversalSingleStorageJdbcQueue queue = new UniversalSingleStorageJdbcQueue()) {
                queue.setTableName("queuess").setTableNameEphemeral("queuess_ephemeral")
                        .setJdbcHelper(jdbcHelper)
                        .setQueueName(QndQueueSingleStoragePgSQL.class.getSimpleName()).init();

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

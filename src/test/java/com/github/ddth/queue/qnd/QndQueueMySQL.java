package com.github.ddth.queue.qnd;

import java.util.Date;

import org.apache.commons.dbcp2.BasicDataSource;

import com.github.ddth.queue.UniversalQueueMessage;
import com.github.ddth.queue.impl.UniversalJdbcQueue;

public class QndQueueMySQL {

    public static void main(String[] args) throws Exception {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource
                .setUrl("jdbc:mysql://localhost:3306/temp?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8");
        dataSource.setUsername("test");
        dataSource.setPassword("test");

        final UniversalJdbcQueue queue = new UniversalJdbcQueue();
        queue.setTableName("queue").setTableNameEphemeral("queue_ephemeral")
                .setDataSource(dataSource).init();

        UniversalQueueMessage msg = new UniversalQueueMessage();
        String content = "Content: [" + System.currentTimeMillis() + "] " + new Date();
        msg.qNumRequeues(0).qOriginalTimestamp(new Date()).qTimestamp(new Date())
                .content(content.getBytes());
        System.out.println("Queue: " + queue.queue(msg));

        msg = queue.take();
        while (msg.qNumRequeues() < 2) {
            System.out.println(msg);
            System.out.println("Requeue: " + queue.requeue(msg));
            msg = queue.take();
        }

        queue.finish(msg);

    }
}

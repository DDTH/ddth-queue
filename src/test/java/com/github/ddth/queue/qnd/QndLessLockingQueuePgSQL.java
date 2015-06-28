package com.github.ddth.queue.qnd;

import java.util.Date;

import org.apache.commons.dbcp2.BasicDataSource;

import com.github.ddth.queue.UniversalQueueMessage;
import com.github.ddth.queue.impl.LessLockingUniversalPgSQLQueue;

public class QndLessLockingQueuePgSQL {

    public static void main(String[] args) throws Exception {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("org.postgresql.Driver");
        dataSource
                .setUrl("jdbc:postgresql://localhost:5432/temp?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8");
        dataSource.setUsername("test");
        dataSource.setPassword("test");

        final LessLockingUniversalPgSQLQueue queue = new LessLockingUniversalPgSQLQueue();
        queue.setTableName("queuell").setDataSource(dataSource).init();

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

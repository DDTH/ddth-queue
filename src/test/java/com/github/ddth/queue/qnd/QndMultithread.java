package com.github.ddth.queue.qnd;

import java.util.Date;

import org.apache.commons.dbcp2.BasicDataSource;

import com.github.ddth.queue.IQueueMessage;

public class QndMultithread {

    public static void main(String[] args) throws Exception {
        BasicDataSource dataSource = new BasicDataSource();
        dataSource.setDriverClassName("com.mysql.jdbc.Driver");
        dataSource
                .setUrl("jdbc:mysql://localhost:3306/queue?autoReconnect=true&useUnicode=true&characterEncoding=UTF-8");
        dataSource.setUsername("root");
        dataSource.setPassword("");

        final MyJdbcQueue queue = new MyJdbcQueue();
        queue.setTableName("queue").setTableNameEphemeral("queue_ephemeral")
                .setDataSource(dataSource).init();

        for (int i = 0; i < 2; i++) {
            Thread t = new Thread() {
                public void run() {
                    while (true) {
                        IQueueMessage msg = queue.take();
                        if (msg != null) {
                            System.out.println(this + ": " + msg);
                            queue.finish(msg);
                        } else {
                            try {
                                Thread.sleep(100);
                            } catch (InterruptedException e) {
                            }
                        }
                    }
                }
            };
            t.start();
        }

        Thread.sleep(3000);
        MyQueueMessage msg = new MyQueueMessage();
        msg.qId(System.currentTimeMillis()).qNumRequeues(0).qOriginalTimestamp(new Date())
                .qTimestamp(new Date()).content("Content: " + new Date());
        System.out.println("Sending: " + msg);
        queue.queue(msg);
    }
}

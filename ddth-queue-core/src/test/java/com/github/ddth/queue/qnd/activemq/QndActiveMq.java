package com.github.ddth.queue.qnd.activemq;

import org.apache.activemq.ActiveMQConnectionFactory;

import javax.jms.*;

public class QndActiveMq {
    static void send(Connection connection, String queueName, String msg) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(queueName);

        MessageProducer producer = session.createProducer(destination);
        Message message = session.createTextMessage(msg);
        producer.send(message);

        session.close();
    }

    static void send(Connection connection, String queueName, byte[] msg) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        Destination destination = session.createQueue(queueName);
        MessageProducer producer = session.createProducer(destination);
        BytesMessage message = session.createBytesMessage();
        message.writeBytes(msg);
        producer.send(message);
        session.close();
    }

    static Message consume(Connection connection, String queueName) throws Exception {
        Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
        try {
            Destination destination = session.createQueue(queueName);
            MessageConsumer consumer = session.createConsumer(destination);
            return consumer.receiveNoWait();
        } finally {
            session.close();
        }
    }

    public static void main(String[] args) throws Exception {
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(
                "tcp://localhost:61616");
        Connection connection = connectionFactory.createConnection();
        connection.start();

        String queueName = "ddth-queue";
        String msg = "Hello world! From: " + Thread.currentThread().getName() + " : " + System
                .currentTimeMillis();

        send(connection, queueName, msg);
        send(connection, queueName, msg.getBytes());

        Message qmsg = consume(connection, queueName);
        while (qmsg != null) {
            System.out.println(qmsg);
            qmsg = consume(connection, queueName);
        }

        connection.close();
    }
}

package com.github.ddth.queue.impl;

import java.util.Collection;
import java.util.Date;

import javax.jms.BytesMessage;
import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.StringUtils;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;

/**
 * (Experimental) ActiveMQ implementation of {@link IQueue}.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.1
 */
public abstract class ActiveMqQueue<ID, DATA> extends AbstractQueue<ID, DATA> {

    public final static String DEFAULT_URI = "tcp://localhost:61616";
    public final static String DEFAULT_QUEUE_NAME = "ddth-queue";

    private ActiveMQConnectionFactory connectionFactory;
    private boolean myOwnConnectionFactory = true;
    private String uri = DEFAULT_URI, username, password;
    private String queueName = DEFAULT_QUEUE_NAME;
    private Connection connection;

    /**
     * Get ActiveMQ's connection URI (see
     * http://activemq.apache.org/connection-configuration-uri.html).
     *
     * @return
     */
    public String getUri() {
        return uri;
    }

    /**
     * Set ActiveMQ's connection URI (see
     * http://activemq.apache.org/connection-configuration-uri.html).
     *
     * @param uri
     * @return
     */
    public ActiveMqQueue<ID, DATA> setUri(String uri) {
        this.uri = uri;
        return this;
    }

    public String getUsername() {
        return username;
    }

    public ActiveMqQueue<ID, DATA> setUsername(String username) {
        this.username = username;
        return this;
    }

    public String getPassword() {
        return password;
    }

    public ActiveMqQueue<ID, DATA> setPassword(String password) {
        this.password = password;
        return this;
    }

    /**
     * Name of ActiveMQ queue to send/receive messages.
     *
     * @return
     */
    public String getQueueName() {
        return queueName;
    }

    public ActiveMqQueue<ID, DATA> setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    protected ActiveMQConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    /**
     * Setter for {@link #connectionFactory}.
     * 
     * @param connectionFactory
     * @param setMyOwnConnectionFactory
     * @return
     * @since 0.7.1
     */
    protected ActiveMqQueue<ID, DATA> setConnectionFactory(
            ActiveMQConnectionFactory connectionFactory, boolean setMyOwnConnectionFactory) {
        if (myOwnConnectionFactory && this.connectionFactory != null) {
            // destroy this.connectionFactory
        }
        this.connectionFactory = connectionFactory;
        myOwnConnectionFactory = setMyOwnConnectionFactory;
        return this;
    }

    public ActiveMqQueue<ID, DATA> setConnectionFactory(
            ActiveMQConnectionFactory connectionFactory) {
        return setConnectionFactory(connectionFactory, false);
    }

    protected Connection getConnection() throws JMSException {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    connection = StringUtils.isEmpty(username)
                            ? connectionFactory.createConnection()
                            : connectionFactory.createConnection(getUsername(), getPassword());
                    connection.start();
                }
            }
        }
        return connection;
    }

    protected Session createSession(int acknowledgeMode) throws JMSException {
        return getConnection().createSession(false, acknowledgeMode);
    }

    private Session producerSession;
    private MessageProducer messageProducer;

    /**
     * Get the {@link Session} dedicated for sending messages.
     *
     * @return
     * @throws JMSException
     */
    protected Session getProducerSession() throws JMSException {
        if (producerSession == null) {
            synchronized (this) {
                if (producerSession == null) {
                    producerSession = createSession(Session.AUTO_ACKNOWLEDGE);
                }
            }
        }
        return producerSession;
    }

    protected MessageProducer getMessageProducer() throws JMSException {
        if (messageProducer == null) {
            synchronized (this) {
                if (messageProducer == null) {
                    Session session = getProducerSession();
                    Destination destination = session.createQueue(queueName);
                    messageProducer = session.createProducer(destination);
                }
            }
        }
        return messageProducer;
    }

    private Session consumerSession;
    private MessageConsumer messageConsumer;

    /**
     * Get the {@link Session} dedicated for consuming messages.
     *
     * @return
     * @throws JMSException
     */
    protected Session getConsumerSession() throws JMSException {
        if (consumerSession == null) {
            synchronized (this) {
                if (consumerSession == null) {
                    consumerSession = createSession(Session.AUTO_ACKNOWLEDGE);
                }
            }
        }
        return consumerSession;
    }

    protected MessageConsumer getMessageConsumer() throws JMSException {
        if (messageConsumer == null) {
            synchronized (this) {
                if (messageConsumer == null) {
                    Session session = getConsumerSession();
                    Destination destination = session.createQueue(queueName);
                    messageConsumer = session.createConsumer(destination);
                }
            }
        }
        return messageConsumer;
    }

    /*----------------------------------------------------------------------*/

    /**
     * 
     * @return
     * @since 0.6.2.6
     */
    protected ActiveMQConnectionFactory buildConnectionFactory() {
        String uri = getUri();
        if (StringUtils.isBlank(uri)) {
            throw new IllegalStateException("ActiveMQ Broker URI is not defined.");
        }
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory(uri);
        return cf;
    }

    /**
     * Init method.
     *
     * @return
     * @throws Exception
     */
    public ActiveMqQueue<ID, DATA> init() throws Exception {
        if (connectionFactory == null) {
            setConnectionFactory(buildConnectionFactory(), true);
        }

        super.init();

        if (connectionFactory == null) {
            throw new IllegalStateException("ActiveMQ Connection factory is null.");
        }

        return this;
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        try {
            super.destroy();
        } finally {
            closeQuietly(connection);
            closeQuietly(messageProducer);
            closeQuietly(producerSession);
            closeQuietly(messageConsumer);
            closeQuietly(consumerSession);
            if (connectionFactory != null && myOwnConnectionFactory) {
                connectionFactory = null;
            }
        }
    }

    protected void closeQuietly(Connection connection) {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (Exception e) {

        }
    }

    protected void closeQuietly(Session session) {
        try {
            if (session != null) {
                session.close();
            }
        } catch (Exception e) {

        }
    }

    protected void closeQuietly(MessageConsumer consumer) {
        try {
            if (consumer != null) {
                consumer.close();
            }
        } catch (Exception e) {

        }
    }

    protected void closeQuietly(MessageProducer producer) {
        try {
            if (producer != null) {
                producer.close();
            }
        } catch (Exception e) {

        }
    }

    /**
     * Puts a message to ActiveMQ queue.
     *
     * @param msg
     * @return
     */
    protected boolean putToQueue(IQueueMessage<ID, DATA> msg) {
        try {
            BytesMessage message = getProducerSession().createBytesMessage();
            message.writeBytes(serialize(msg));
            getMessageProducer().send(message);
            return true;
        } catch (Exception e) {
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean queue(IQueueMessage<ID, DATA> _msg) {
        IQueueMessage<ID, DATA> msg = _msg.clone();
        Date now = new Date();
        msg.setNumRequeues(0).setQueueTimestamp(now).setTimestamp(now);
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeue(IQueueMessage<ID, DATA> _msg) {
        IQueueMessage<ID, DATA> msg = _msg.clone();
        Date now = new Date();
        msg.incNumRequeues().setQueueTimestamp(now);
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeueSilent(IQueueMessage<ID, DATA> _msg) {
        IQueueMessage<ID, DATA> msg = _msg.clone();
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage<ID, DATA> msg) {
        // EMPTY
    }

    /**
     * {@inheritDoc}
     *
     * @throws QueueException.EphemeralIsFull
     *             if the ephemeral storage is full
     */
    @Override
    public IQueueMessage<ID, DATA> take() throws QueueException.EphemeralIsFull {
        try {
            MessageConsumer consumer = getMessageConsumer();
            synchronized (consumer) {
                // Message message = consumer.receiveNoWait();
                Message message = consumer.receive(1000);
                if (message instanceof BytesMessage) {
                    BytesMessage msg = (BytesMessage) message;
                    byte[] buff = new byte[(int) msg.getBodyLength()];
                    msg.readBytes(buff);
                    return deserialize(buff);
                } else {
                    return null;
                }
            }
        } catch (Exception e) {
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<IQueueMessage<ID, DATA>> getOrphanMessages(long thresholdTimestampMs) {
        throw new QueueException.OperationNotSupported(
                "This queue does not support retrieving orphan messages.");
    }

//    /**
//     * {@inheritDoc}
//     */
//    @Override
//    public boolean moveFromEphemeralToQueueStorage(IQueueMessage<ID, DATA> msg) {
//        throw new QueueException.OperationNotSupported(
//                "This queue does not support ephemeral storage.");
//    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        return SIZE_NOT_SUPPORTED;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        return SIZE_NOT_SUPPORTED;
    }
}

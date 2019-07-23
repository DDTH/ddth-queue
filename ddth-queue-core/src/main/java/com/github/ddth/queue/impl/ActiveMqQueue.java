package com.github.ddth.queue.impl;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.commons.lang3.StringUtils;

import javax.jms.*;
import java.lang.IllegalStateException;
import java.util.Collection;

/**
 * (Experimental) ActiveMQ implementation of {@link IQueue}.
 *
 * <ul>
 * <li>Queue-size support: no</li>
 * <li>Ephemeral storage support: no</li>
 * </ul>
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
     * ActiveMQ's connection URI (see http://activemq.apache.org/connection-configuration-uri.html).
     *
     * @return
     */
    public String getUri() {
        return uri;
    }

    /**
     * ActiveMQ's connection URI (see http://activemq.apache.org/connection-configuration-uri.html).
     *
     * @param uri
     * @return
     */
    public ActiveMqQueue<ID, DATA> setUri(String uri) {
        this.uri = uri;
        return this;
    }

    /**
     * Username to connect to ActiveMQ server.
     *
     * @return
     */
    public String getUsername() {
        return username;
    }

    /**
     * Username to connect to ActiveMQ server.
     *
     * @param username
     * @return
     */
    public ActiveMqQueue<ID, DATA> setUsername(String username) {
        this.username = username;
        return this;
    }

    /**
     * Password to connect to ActiveMQ server.
     *
     * @return
     */
    public String getPassword() {
        return password;
    }

    /**
     * Password to connect to ActiveMQ server.
     *
     * @param password
     * @return
     */
    public ActiveMqQueue<ID, DATA> setPassword(String password) {
        this.password = password;
        return this;
    }

    /**
     * Name of ActiveMQ queue to send/receive messages.
     *
     * @return
     */
    @Override
    public String getQueueName() {
        return queueName;
    }

    /**
     * Name of ActiveMQ queue to send/receive messages.
     *
     * @param queueName
     * @return
     */
    @Override
    public ActiveMqQueue<ID, DATA> setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    /**
     * Getter for {@link #connectionFactory}.
     *
     * @return
     */
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
    protected ActiveMqQueue<ID, DATA> setConnectionFactory(ActiveMQConnectionFactory connectionFactory,
            boolean setMyOwnConnectionFactory) {
        if (myOwnConnectionFactory && this.connectionFactory != null) {
            // destroy this.connectionFactory
        }
        this.connectionFactory = connectionFactory;
        myOwnConnectionFactory = setMyOwnConnectionFactory;
        return this;
    }

    /**
     * Setter for {@link #connectionFactory}.
     *
     * @param connectionFactory
     * @return
     */
    public ActiveMqQueue<ID, DATA> setConnectionFactory(ActiveMQConnectionFactory connectionFactory) {
        return setConnectionFactory(connectionFactory, false);
    }

    protected Connection getConnection() throws JMSException {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    connection = StringUtils.isEmpty(username) ?
                            connectionFactory.createConnection() :
                            connectionFactory.createConnection(getUsername(), getPassword());
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
            throw new IllegalStateException("ActiveMQ connection factory is null.");
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
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage<ID, DATA> msg) {
        //do nothing as we use Session.AUTO_ACKNOWLEDGE
    }

    /**
     * {@inheritDoc}
     * <p>{@code queueCase} is ignore as we always add new message to ActiveMQ.</p>
     */
    @Override
    protected boolean doPutToQueue(IQueueMessage<ID, DATA> msg, PutToQueueCase queueCase) {
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
     *
     * @throws QueueException.EphemeralIsFull if the ephemeral storage is full
     */
    @Override
    public IQueueMessage<ID, DATA> take() throws QueueException.EphemeralIsFull {
        try {
            MessageConsumer consumer = getMessageConsumer();
            synchronized (consumer) {
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
        throw new QueueException.OperationNotSupported("This queue does not support retrieving orphan messages.");
    }

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

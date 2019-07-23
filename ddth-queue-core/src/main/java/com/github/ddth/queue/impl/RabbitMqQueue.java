package com.github.ddth.queue.impl;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.concurrent.TimeoutException;

/**
 * (Experimental) RabbitMQ implementation of {@link IQueue}.
 *
 * <ul>
 * <li>Queue-size support: no</li>
 * <li>Ephemeral storage support: no</li>
 * </ul>
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.1
 */
public abstract class RabbitMqQueue<ID, DATA> extends AbstractQueue<ID, DATA> {

    public final static String DEFAULT_URI = "amqp://localhost:5672";
    public final static String DEFAULT_QUEUE_NAME = "ddth-queue";

    private ConnectionFactory connectionFactory;
    private boolean myOwnConnectionFactory = true;
    private String uri = DEFAULT_URI;
    private String queueName = DEFAULT_QUEUE_NAME;
    private Connection connection;

    /**
     * RabbitMQ's connection URI (format {@code amqp://username:password@host:port/virtualHost}).
     *
     * @return
     */
    public String getUri() {
        return uri;
    }

    /**
     * RabbitMQ's connection URI (format {@code amqp://username:password@host:port/virtualHost}).
     *
     * @param uri
     * @return
     */
    public RabbitMqQueue<ID, DATA> setUri(String uri) {
        this.uri = uri;
        return this;
    }

    /**
     * Name of RabbitMQ queue to send/receive messages.
     *
     * @return
     */
    public String getQueueName() {
        return queueName;
    }

    /**
     * Name of RabbitMQ queue to send/receive messages.
     *
     * @param queueName
     * @return
     */
    public RabbitMqQueue<ID, DATA> setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    /**
     * Getter for {@link #connectionFactory}.
     *
     * @return
     */
    protected ConnectionFactory getConnectionFactory() {
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
    protected RabbitMqQueue<ID, DATA> setConnectionFactory(ConnectionFactory connectionFactory,
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
    public RabbitMqQueue<ID, DATA> setConnectionFactory(ConnectionFactory connectionFactory) {
        return setConnectionFactory(connectionFactory, false);
    }

    protected Connection getConnection() throws IOException, TimeoutException {
        if (connection == null) {
            synchronized (this) {
                if (connection == null) {
                    connection = connectionFactory.newConnection();
                }
            }
        }
        return connection;
    }

    protected Channel createChannel() throws IOException, TimeoutException {
        return getConnection().createChannel();
    }

    private Channel producerChannel;

    /**
     * Get the {@link Channel} dedicated for sending messages.
     *
     * @return
     * @throws IOException
     * @throws TimeoutException
     */
    protected Channel getProducerChannel() throws IOException, TimeoutException {
        if (producerChannel == null) {
            synchronized (this) {
                if (producerChannel == null) {
                    producerChannel = createChannel();
                }
            }
        }
        return producerChannel;
    }

    private Channel consumerChannel;

    /**
     * Get the {@link Channel} dedicated for consuming messages.
     *
     * @return
     * @throws IOException
     * @throws TimeoutException
     */
    protected Channel getConsumerChannel() throws IOException, TimeoutException {
        if (consumerChannel == null) {
            synchronized (this) {
                if (consumerChannel == null) {
                    consumerChannel = createChannel();
                }
            }
        }
        return consumerChannel;
    }

    /*----------------------------------------------------------------------*/

    /**
     * @return
     * @throws URISyntaxException
     * @throws NoSuchAlgorithmException
     * @throws KeyManagementException
     * @since 0.6.2.6
     */
    protected ConnectionFactory buildConnectionFactory()
            throws KeyManagementException, NoSuchAlgorithmException, URISyntaxException {
        String uri = getUri();
        if (StringUtils.isBlank(uri)) {
            throw new IllegalStateException("RabbitMQ Broker URI is not defined.");
        }
        ConnectionFactory cf = new ConnectionFactory();
        cf.setUri(uri);
        return cf;
    }

    /**
     * Init method.
     *
     * @return
     * @throws Exception
     */
    public RabbitMqQueue<ID, DATA> init() throws Exception {
        if (connectionFactory == null) {
            setConnectionFactory(buildConnectionFactory(), true);
        }

        super.init();

        if (connectionFactory == null) {
            throw new IllegalStateException("RabbitMQ connection factory is null.");
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
            closeQuietly(producerChannel);
            closeQuietly(consumerChannel);
            if (connectionFactory != null && myOwnConnectionFactory) {
                connectionFactory = null;
            }
        }
    }

    protected void closeQuietly(Connection connection) {
        if (connection != null) {
            try {
                connection.close();
            } catch (Exception e) {
            }
        }
    }

    protected void closeQuietly(Channel channel) {
        if (channel != null) {
            try {
                channel.close();
            } catch (Exception e) {
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage<ID, DATA> msg) {
        //do nothing as we use 'autoAck'
    }

    /**
     * {@inheritDoc}
     * <p>{@code queueCase} is ignore as we always add new message to RabbitMQ.</p>
     */
    @Override
    protected boolean doPutToQueue(IQueueMessage<ID, DATA> msg, PutToQueueCase queueCase) {
        try {
            byte[] msgData = serialize(msg);
            getProducerChannel().basicPublish("" /*exchange*/, queueName, null /*basic-properties*/, msgData);
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
            GetResponse msg = getConsumerChannel().basicGet(queueName, true);
            return msg != null ? deserialize(msg.getBody()) : null;
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
        try (Channel channel = createChannel()) {
            return channel.queueDeclarePassive(getQueueName()).getMessageCount();
        } catch (Exception e) {
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        return SIZE_NOT_SUPPORTED;
    }
}

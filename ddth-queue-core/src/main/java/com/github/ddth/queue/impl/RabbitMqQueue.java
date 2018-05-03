package com.github.ddth.queue.impl;

import java.io.IOException;
import java.net.URISyntaxException;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.Date;
import java.util.concurrent.TimeoutException;

import org.apache.commons.lang3.StringUtils;

import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;

/**
 * (Experimental) RabbitMQ implementation of {@link IQueue}.
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
     * Get RabbitMQ's connection URI (format
     * {@code amqp://username:password@host:port/virtualHost}).
     *
     * @return
     */
    public String getUri() {
        return uri;
    }

    /**
     * Set RabbitMQ's connection URI (format
     * {@code amqp://username:password@host:port/virtualHost}).
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

    public RabbitMqQueue<ID, DATA> setQueueName(String queueName) {
        this.queueName = queueName;
        return this;
    }

    protected ConnectionFactory getConnectionFactory() {
        return connectionFactory;
    }

    public RabbitMqQueue<ID, DATA> setConnectionFactory(ConnectionFactory connectionFactory) {
        this.connectionFactory = connectionFactory;
        myOwnConnectionFactory = false;
        return this;
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
     * 
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
            connectionFactory = buildConnectionFactory();
            myOwnConnectionFactory = connectionFactory != null;
        }

        super.init();

        if (connectionFactory == null) {
            throw new IllegalStateException("RabbitMQ Connection factory is null.");
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
     * Serializes a queue message to store in Redis.
     *
     * @param msg
     * @return
     */
    protected abstract byte[] serialize(IQueueMessage<ID, DATA> msg);

    /**
     * Deserilizes a queue message.
     *
     * @param msgData
     * @return
     */
    protected abstract IQueueMessage<ID, DATA> deserialize(byte[] msgData);

    /**
     * Puts a message to Kafka queue, partitioning message by
     * {@link IQueueMessage#qId()}
     *
     * @param msg
     * @return
     */
    protected boolean putToQueue(IQueueMessage<ID, DATA> msg) {
        try {
            byte[] msgData = serialize(msg);
            getProducerChannel().basicPublish("", queueName, null, msgData);
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
        msg.qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now);
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeue(IQueueMessage<ID, DATA> _msg) {
        IQueueMessage<ID, DATA> msg = _msg.clone();
        Date now = new Date();
        msg.qIncNumRequeues().qTimestamp(now);
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

    // protected static class RabbitMQMessage {
    // public final String consumerTag;
    // public final Envelope envelope;
    // public final AMQP.BasicProperties properties;
    // public final byte[] body;
    //
    // public RabbitMQMessage(String consumerTag, Envelope envelope,
    // AMQP.BasicProperties properties, byte[] body) {
    // this.consumerTag = consumerTag;
    // this.envelope = envelope;
    // this.properties = properties;
    // this.body = body;
    // }
    // }

    /**
     * {@inheritDoc}
     *
     * @throws QueueException.EphemeralIsFull
     *             if the ephemeral storage is full
     */
    @Override
    public IQueueMessage<ID, DATA> take() throws QueueException.EphemeralIsFull {
        try {
            GetResponse msg = getConsumerChannel().basicGet(queueName, true);
            return msg != null ? deserialize(msg.getBody()) : null;
        } catch (Exception e) {
            throw e instanceof QueueException ? (QueueException) e : new QueueException(e);
        }

        // try (Channel channel = createChannel()) {
        // GetResponse msg = channel.basicGet(queueName, true);
        // return msg != null ? deserialize(msg.getBody()) : null;
        // } catch (Exception e) {
        // throw e instanceof QueueException ? (QueueException) e : new
        // QueueException(e);
        // }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Collection<IQueueMessage<ID, DATA>> getOrphanMessages(long thresholdTimestampMs) {
        throw new QueueException.OperationNotSupported(
                "This queue does not support retrieving orphan messages.");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean moveFromEphemeralToQueueStorage(IQueueMessage<ID, DATA> msg) {
        throw new QueueException.OperationNotSupported(
                "This queue does not support ephemeral storage.");
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
        return -1;
    }
}

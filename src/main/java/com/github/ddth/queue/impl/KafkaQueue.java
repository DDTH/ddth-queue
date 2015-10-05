package com.github.ddth.queue.impl;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import kafka.consumer.Consumer;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.message.MessageAndMetadata;

import com.github.ddth.kafka.KafkaClient;
import com.github.ddth.kafka.KafkaClient.ProducerType;
import com.github.ddth.kafka.KafkaMessage;
import com.github.ddth.queue.IQueue;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.utils.QueueException;

/**
 * (Experimental) Kafka implementation of {@link IQueue}.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.3.2
 */
public abstract class KafkaQueue implements IQueue {

    private KafkaClient kafkaClient;
    private boolean myOwnKafkaClient = true;

    private String zkConnString = "localhost:2181/kafka";
    private String topicName = "ddth-queue";
    private String consumerGroupId = "kafkaqueue-" + System.currentTimeMillis();
    private ProducerType producerType = ProducerType.SYNC_LEADER_ACK;

    private boolean leaderAutoRebalance = false;
    private ConsumerConnector kafkaConsumer;
    private List<KafkaStream<byte[], byte[]>> kafkaStreams;
    private BlockingQueue<InternalQueueMessage> kafkaConsumerQueue;
    private int bufferSize = 1;
    private long offsetCommitPeriodMs = 1000;
    private AtomicLong offsetsCommitTokens = new AtomicLong(0);

    public ProducerType getProducerType() {
        return producerType;
    }

    public KafkaQueue setProducerType(ProducerType producerType) {
        this.producerType = producerType;
        return this;
    }

    /**
     * Zookeeper connection string (format
     * {@code host1:port1,host2:port2,host3:port3/path}).
     * 
     * @return
     */
    public String getZkConnString() {
        return zkConnString;
    }

    /**
     * Sets Zookeeper connection string (format
     * {@code host1:port1,host2:port2,host3:port3/path}).
     * 
     * @param redisHostAndPort
     * @return
     */
    public KafkaQueue setZkConnString(String zkConnString) {
        this.zkConnString = zkConnString;
        return this;
    }

    /**
     * Name of Kafka topic to store queue messages.
     * 
     * @return
     */
    public String getTopicName() {
        return topicName;
    }

    public KafkaQueue setTopicName(String topicName) {
        this.topicName = topicName;
        return this;
    }

    /**
     * Should we allow leaders to rebalance? Default value is {@code false}.
     * 
     * @return
     */
    public boolean isLeaderAutoRebalance() {
        return leaderAutoRebalance;
    }

    public KafkaQueue setLeaderAutoRebalance(boolean leaderAutoRebalance) {
        this.leaderAutoRebalance = leaderAutoRebalance;
        return this;
    }

    /**
     * Kafka's group-id to consume messages.
     * 
     * @return
     */
    public String getConsumerGroupId() {
        return consumerGroupId;
    }

    public KafkaQueue setConsumerGroupId(String consumerGroupId) {
        this.consumerGroupId = consumerGroupId;
        return this;
    }

    /**
     * Number of maximum items {@link KafkaQueue} should buffer. Default value
     * is {@code 1}. Higher value could gain more performance in consuming
     * messages, but could cause message lost if client crashes.
     * 
     * @return
     */
    public int getBufferSize() {
        return bufferSize;
    }

    public KafkaQueue setBufferSize(int bufferSize) {
        this.bufferSize = bufferSize;
        return this;
    }

    /**
     * Consumer's topic offset will be committed to server every
     * {@link #offsetCommitPeriodMs} milliseconds.Number of maximum items
     * {@link KafkaQueue} should buffer. Default value is {@code 1}. Higher
     * value could gain more performance in consuming messages, but could cause
     * message lost if client crashes.
     * 
     * @return
     */
    public long getOffsetCommitPeriodMs() {
        return offsetCommitPeriodMs;
    }

    public KafkaQueue setOffsetCommitPeriodMs(long offsetCommitPeriodMs) {
        this.offsetCommitPeriodMs = offsetCommitPeriodMs;
        return this;
    }

    protected KafkaClient getKafkaClient() {
        return kafkaClient;
    }

    /**
     * An external {@link KafkaClient} can be used. If not set,
     * {@link KafkaClient} will automatically create a {@link KafkaClient} for
     * its own use.
     * 
     * @param kafkaClient
     * @return
     */
    public KafkaQueue setKafkaClient(KafkaClient kafkaClient) {
        this.kafkaClient = kafkaClient;
        myOwnKafkaClient = false;
        return this;
    }

    /*----------------------------------------------------------------------*/

    /**
     * Init method.
     * 
     * @return
     * @throws Exception
     */
    public KafkaQueue init() throws Exception {
        if (kafkaClient == null) {
            kafkaClient = new KafkaClient(zkConnString);
            kafkaClient.init();
            myOwnKafkaClient = true;
        }

        if (kafkaConsumerQueue == null) {
            kafkaConsumerQueue = new LinkedBlockingDeque<KafkaQueue.InternalQueueMessage>(
                    bufferSize > 0 ? bufferSize : 1);
        }

        if (kafkaConsumer == null) {
            Properties props = new Properties();
            props.put("zookeeper.connect", kafkaClient.getZookeeperConnectString());
            props.put("group.id", consumerGroupId);
            props.put("zookeeper.session.timeout.ms", "600000");
            props.put("zookeeper.connection.timeout.ms", "10000");
            props.put("zookeeper.sync.time.ms", "2000");
            props.put("socket.timeout.ms", "5000");
            props.put("fetch.wait.max.ms", "2000");
            props.put("auto.offset.reset", "smallest");

            props.put("controlled.shutdown.enable", "true");
            props.put("controlled.shutdown.max.retries", "5");
            props.put("controlled.shutdown.retry.backoff.ms", "10000");

            if (leaderAutoRebalance) {
                props.put("auto.leader.rebalance.enable", "true");
                props.put("rebalance.backoff.ms", "10000");
                props.put("refresh.leader.backoff.ms", "1000");
            } else {
                props.put("auto.leader.rebalance.enable", "false");
            }
            ConsumerConfig consumerConfig = new ConsumerConfig(props);
            kafkaConsumer = Consumer.createJavaConsumerConnector(consumerConfig);

            Map<String, Integer> topicCountMap = new HashMap<String, Integer>();
            int numStreams = kafkaClient.getTopicNumPartitions(topicName);
            if (numStreams < 1) {
                numStreams = 1;
            }
            if (numStreams > 8) {
                numStreams = 8;
            }
            topicCountMap.put(topicName, new Integer(numStreams));
            Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = kafkaConsumer
                    .createMessageStreams(topicCountMap);
            kafkaStreams = consumerMap.get(topicName);
            consumerThreads = new KafkaConsumerThread[kafkaStreams.size()];
            for (int i = 0; i < consumerThreads.length; i++) {
                consumerThreads[i] = new KafkaConsumerThread(i, kafkaConsumerQueue,
                        kafkaStreams.get(i));
                consumerThreads[i].start();
            }
        }

        offsetCommitterThread = new KafkaOffsetCommitterThread();
        offsetCommitterThread.start();

        return this;
    }

    /**
     * Destroy method.
     */
    public void destroy() {
        if (consumerThreads != null) {
            for (KafkaConsumerThread consumerThread : consumerThreads) {
                try {
                    consumerThread.stopThread();
                } catch (Exception e) {
                }
            }
            consumerThreads = null;
        }

        if (offsetCommitterThread != null) {
            try {
                offsetCommitterThread.stopThread();
            } catch (Exception e) {
            } finally {
                offsetCommitterThread = null;
            }
        }

        if (kafkaConsumer != null) {
            try {
                kafkaConsumer.shutdown();
            } catch (Exception e) {
            } finally {
                kafkaConsumer = null;
            }
        }

        if (kafkaClient != null && myOwnKafkaClient) {
            try {
                kafkaClient.destroy();
            } catch (Exception e) {

            } finally {
                kafkaClient = null;
            }
        }
    }

    private static class InternalQueueMessage {
        public MessageAndMetadata<byte[], byte[]> mm;

        public InternalQueueMessage(MessageAndMetadata<byte[], byte[]> mm) {
            this.mm = mm;
        }
    }

    private KafkaOffsetCommitterThread offsetCommitterThread;

    private class KafkaOffsetCommitterThread extends Thread {
        private boolean stop = false;

        public KafkaOffsetCommitterThread() {
            super("KafkaQueue - OffsetCommitterThread");
        }

        public void stopThread() {
            stop = true;
        }

        public void run() {
            while (!stop && !isInterrupted()) {
                try {
                    Thread.sleep(offsetCommitPeriodMs);
                    if (offsetsCommitTokens.get() > 0) {
                        try {
                            kafkaConsumer.commitOffsets();
                        } finally {
                            offsetsCommitTokens.decrementAndGet();
                        }
                    }
                } catch (Exception e) {
                }
            }
        }
    }

    private KafkaConsumerThread[] consumerThreads;

    private class KafkaConsumerThread extends Thread {
        private KafkaStream<byte[], byte[]> stream;
        private BlockingQueue<InternalQueueMessage> consumerQueue;
        private boolean stop = false;

        public KafkaConsumerThread(int id, BlockingQueue<InternalQueueMessage> consumerQueue,
                KafkaStream<byte[], byte[]> stream) {
            super("KafkaQueue - ConsumerThread - " + id);
            this.consumerQueue = consumerQueue;
            this.stream = stream;
        }

        public void stopThread() {
            stop = true;
        }

        @Override
        public void run() {
            while (!stop && !isInterrupted()) {
                ConsumerIterator<byte[], byte[]> it = stream.iterator();
                if (it.hasNext()) {
                    MessageAndMetadata<byte[], byte[]> mm = it.next();
                    if (mm != null) {
                        boolean offered = false;
                        while (!offered && !stop) {
                            try {
                                offered = consumerQueue.offer(new InternalQueueMessage(mm), 10,
                                        TimeUnit.MILLISECONDS);
                                Thread.yield();
                            } catch (InterruptedException e) {
                            }
                        }
                    }
                }
                Thread.yield();
            }
        }
    }

    /**
     * Serializes a queue message to store in Kafka.
     * 
     * @param msg
     * @return
     */
    protected abstract byte[] serialize(IQueueMessage msg) throws QueueException;

    /**
     * Deserilizes a queue message.
     * 
     * @param msgData
     * @return
     */
    protected abstract IQueueMessage deserialize(byte[] msgData) throws QueueException;

    /**
     * Takes a message from Kafka queue.
     * 
     * @return
     * @since 0.3.3
     */
    protected IQueueMessage takeFromQueue() {
        InternalQueueMessage iQueueMsg = kafkaConsumerQueue.poll();
        if (iQueueMsg != null) {
            MessageAndMetadata<byte[], byte[]> mm = iQueueMsg.mm;
            KafkaMessage kmsg = new KafkaMessage(mm);
            offsetsCommitTokens.incrementAndGet();
            return kmsg != null ? deserialize(kmsg.content()) : null;
        }
        return null;
    }

    /**
     * Puts a message to Kafka queue, partitioning message by
     * {@link IQueueMessage#qId()}
     * 
     * @param msg
     * @return
     */
    protected boolean putToQueue(IQueueMessage msg) {
        byte[] msgData = serialize(msg);
        Object qId = msg.qId();
        KafkaMessage kmsg = qId != null ? new KafkaMessage(topicName, qId.toString(), msgData)
                : new KafkaMessage(topicName, msgData);
        kafkaClient.sendMessage(producerType, kmsg);
        return true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean queue(IQueueMessage msg) {
        Date now = new Date();
        msg.qNumRequeues(0).qOriginalTimestamp(now).qTimestamp(now);
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeue(IQueueMessage msg) {
        Date now = new Date();
        msg.qIncNumRequeues().qTimestamp(now);
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean requeueSilent(IQueueMessage msg) {
        return putToQueue(msg);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void finish(IQueueMessage msg) {
        // EMPTY
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IQueueMessage take() {
        return takeFromQueue();
    }

    /**
     * {@inheritDoc}
     * 
     * @param thresholdTimestampMs
     * @return
     */
    @Override
    public Collection<IQueueMessage> getOrphanMessages(long thresholdTimestampMs) {
        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean moveFromEphemeralToQueueStorage(IQueueMessage msg) {
        throw new UnsupportedOperationException(
                "Method [moveFromEphemeralToQueueStorage] has not implemented yet!");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int queueSize() {
        return -1;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int ephemeralSize() {
        return -1;
    }
}

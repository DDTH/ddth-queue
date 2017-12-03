package com.github.ddth.queue.impl;

import java.util.Date;

import com.github.ddth.queue.IPartitionSupport;
import com.github.ddth.queue.IQueueMessage;

/**
 * A generic implementation of {@link IQueueMessage}
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.6.0
 */
public class GenericQueueMessage<ID, DATA>
        implements IQueueMessage<ID, DATA>, Cloneable, IPartitionSupport {

    private ID id;
    private DATA data;
    private Date orgTimestamp = new Date(), timestamp = new Date();
    private int numRequeues = 0;
    private String partitionKey;

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public GenericQueueMessage<ID, DATA> clone() {
        try {
            return (GenericQueueMessage<ID, DATA>) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ID qId() {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericQueueMessage<ID, DATA> qId(ID queueId) {
        this.id = queueId;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date qOriginalTimestamp() {
        return orgTimestamp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericQueueMessage<ID, DATA> qOriginalTimestamp(Date timestamp) {
        this.orgTimestamp = timestamp;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date qTimestamp() {
        return timestamp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericQueueMessage<ID, DATA> qTimestamp(Date timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int qNumRequeues() {
        return numRequeues;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericQueueMessage<ID, DATA> qNumRequeues(int numRequeues) {
        synchronized (this) {
            this.numRequeues = numRequeues;
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericQueueMessage<ID, DATA> qIncNumRequeues() {
        synchronized (this) {
            numRequeues++;
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DATA qData() {
        return data;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericQueueMessage<ID, DATA> qData(DATA data) {
        this.data = data;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String qPartitionKey() {
        return partitionKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public IPartitionSupport qPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
        return this;
    }

}

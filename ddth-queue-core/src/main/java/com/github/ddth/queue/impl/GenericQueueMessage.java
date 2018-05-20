package com.github.ddth.queue.impl;

import java.util.Date;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.github.ddth.commons.utils.DateFormatUtils;
import com.github.ddth.queue.IQueueMessage;

/**
 * A generic implementation of {@link IQueueMessage}
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.6.0
 */
public class GenericQueueMessage<ID, DATA> extends GenericMessage<ID, DATA>
        implements IQueueMessage<ID, DATA> {

    public static <ID, DATA> GenericQueueMessage<ID, DATA> newInstance() {
        return new GenericQueueMessage<>();
    }

    public static <ID, DATA> GenericQueueMessage<ID, DATA> newInstance(DATA data) {
        GenericQueueMessage<ID, DATA> msg = newInstance();
        msg.setData(data);
        return msg;
    }

    public static <ID, DATA> GenericQueueMessage<ID, DATA> newInstance(ID id, DATA data) {
        GenericQueueMessage<ID, DATA> msg = newInstance();
        msg.setId(id).setData(data);
        return msg;
    }

    private Date queueTimestamp = new Date();
    private int numRequeues = 0;

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericQueueMessage<ID, DATA> clone() {
        return (GenericQueueMessage<ID, DATA>) super.clone();
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public ID qId() {
        return getId();
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public GenericQueueMessage<ID, DATA> qId(ID queueId) {
        setId(queueId);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public Date qOriginalTimestamp() {
        return getTimestamp();
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public GenericQueueMessage<ID, DATA> qOriginalTimestamp(Date timestamp) {
        setTimestamp(timestamp);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public Date qTimestamp() {
        return getQueueTimestamp();
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public GenericQueueMessage<ID, DATA> qTimestamp(Date timestamp) {
        return setQueueTimestamp(timestamp);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getQueueTimestamp() {
        return queueTimestamp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericQueueMessage<ID, DATA> setQueueTimestamp(Date timestamp) {
        this.queueTimestamp = timestamp;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public int qNumRequeues() {
        return getNumRequeues();
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public GenericQueueMessage<ID, DATA> qNumRequeues(int numRequeues) {
        return setNumRequeues(numRequeues);
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public GenericQueueMessage<ID, DATA> qIncNumRequeues() {
        return incNumRequeues();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int getNumRequeues() {
        return numRequeues;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericQueueMessage<ID, DATA> setNumRequeues(int numRequeues) {
        synchronized (this) {
            this.numRequeues = numRequeues;
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericQueueMessage<ID, DATA> incNumRequeues() {
        synchronized (this) {
            numRequeues++;
        }
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public DATA qData() {
        return getData();
    }

    /**
     * {@inheritDoc}
     */
    @Deprecated
    @Override
    public GenericQueueMessage<ID, DATA> qData(DATA data) {
        setData(data);
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
        tsb.append("queue_time",
                DateFormatUtils.toString(queueTimestamp, DateFormatUtils.DF_ISO8601))
                .append("num_requeues", numRequeues).appendSuper(super.toString());
        return tsb.toString();
    }
}

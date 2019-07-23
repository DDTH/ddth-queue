package com.github.ddth.queue.impl;

import com.github.ddth.commons.utils.DateFormatUtils;
import com.github.ddth.queue.IQueueMessage;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.Date;

/**
 * A generic implementation of {@link IQueueMessage}
 *
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.6.0
 */
public class GenericQueueMessage<ID, DATA> extends GenericMessage<ID, DATA> implements IQueueMessage<ID, DATA> {

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
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
        tsb.append("queue_time", DateFormatUtils.toString(queueTimestamp, DateFormatUtils.DF_ISO8601))
                .append("num_requeues", numRequeues).appendSuper(super.toString());
        return tsb.toString();
    }
}

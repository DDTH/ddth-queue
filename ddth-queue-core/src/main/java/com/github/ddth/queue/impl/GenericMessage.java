package com.github.ddth.queue.impl;

import java.util.Date;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import com.github.ddth.commons.utils.DateFormatUtils;
import com.github.ddth.queue.IMessage;
import com.github.ddth.queue.IPartitionSupport;

/**
 * A generic implementation of {@link IMessage}
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.7.0
 */
public class GenericMessage<ID, DATA> implements IMessage<ID, DATA>, Cloneable, IPartitionSupport {

    public static <ID, DATA> GenericMessage<ID, DATA> newInstance() {
        return new GenericMessage<>();
    }

    public static <ID, DATA> GenericMessage<ID, DATA> newInstance(DATA data) {
        GenericMessage<ID, DATA> msg = newInstance();
        msg.setData(data);
        return msg;
    }

    public static <ID, DATA> GenericMessage<ID, DATA> newInstance(ID id, DATA data) {
        GenericMessage<ID, DATA> msg = newInstance();
        msg.setId(id).setData(data);
        return msg;
    }

    private ID id;
    private DATA data;
    private Date timestamp = new Date();
    private String partitionKey;

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public GenericMessage<ID, DATA> clone() {
        try {
            return (GenericMessage<ID, DATA>) super.clone();
        } catch (CloneNotSupportedException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ID getId() {
        return id;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericMessage<ID, DATA> setId(ID id) {
        this.id = id;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Date getTimestamp() {
        return timestamp;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericMessage<ID, DATA> setTimestamp(Date timestamp) {
        this.timestamp = timestamp;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public DATA getData() {
        return data;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericMessage<ID, DATA> setData(DATA data) {
        this.data = data;
        return this;
    }

    /**
     * {@inheritDoc}
     * 
     * @deprecated
     */
    @Override
    public String qPartitionKey() {
        return getPartitionKey();
    }

    /**
     * {@inheritDoc}
     * 
     * @deprecated
     */
    @Override
    public GenericMessage<ID, DATA> qPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getPartitionKey() {
        return partitionKey;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GenericMessage<ID, DATA> setPartitionKey(String partitionKey) {
        this.partitionKey = partitionKey;
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this, ToStringStyle.SHORT_PREFIX_STYLE);
        tsb.append("id", id).append("data", data)
                .append("time", DateFormatUtils.toString(timestamp, DateFormatUtils.DF_ISO8601))
                .append("partition", partitionKey);
        return tsb.toString();
    }
}

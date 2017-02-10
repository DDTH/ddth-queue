package com.github.ddth.queue;

import java.util.Map.Entry;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.github.ddth.commons.utils.DPathUtils;

public class QueueSpec {

    public final static int NO_BOUNDARY = -1;

    private SortedMap<String, Object> fieldData = new TreeMap<String, Object>();
    public final static String FIELD_EPHEMERAL_DISABLED = "ephemeral_disabled";
    public final static String FIELD_MAX_SIZE = "max_size";
    public final static String FIELD_EPHEMERAL_MAX_SIZE = "ephemeral_max_size";

    public QueueSpec() {
        this(false, NO_BOUNDARY, NO_BOUNDARY);
    }

    public QueueSpec(boolean ephemeralDisabled) {
        this(ephemeralDisabled, NO_BOUNDARY, NO_BOUNDARY);
    }

    public QueueSpec(int maxSize) {
        this(false, maxSize, NO_BOUNDARY);
    }

    public QueueSpec(int maxSize, int ephemeralMaxSize) {
        this(false, maxSize, ephemeralMaxSize);
    }

    public QueueSpec(boolean ephemeralDisabled, int maxSize) {
        this(false, maxSize, NO_BOUNDARY);
    }

    public QueueSpec(boolean ephemeralDisabled, int maxSize, int ephemeralMaxSize) {
        setField(FIELD_EPHEMERAL_DISABLED, ephemeralDisabled);
        setField(FIELD_MAX_SIZE, maxSize);
        setField(FIELD_EPHEMERAL_MAX_SIZE, ephemeralMaxSize);
    }

    /**
     * Sets a field's value.
     * 
     * @param fieldName
     * @param value
     * @return
     */
    public QueueSpec setField(String fieldName, Object value) {
        fieldData.put(fieldName, value);
        return this;
    }

    /**
     * Gets a field's value.
     * 
     * @param fieldName
     * @param clazz
     * @return
     */
    public <T> T getField(String fieldName, Class<T> clazz) {
        return DPathUtils.getValue(fieldData, fieldName, clazz);
    }

    /**
     * Gets a field's value.
     * 
     * @param fieldName
     * @return
     */
    public String getField(String fieldName) {
        return getField(fieldName, String.class);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof QueueSpec) {
            QueueSpec another = (QueueSpec) obj;
            EqualsBuilder eq = new EqualsBuilder();
            eq.append(fieldData, another.fieldData);
            return eq.isEquals();
        }
        return false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        HashCodeBuilder hcb = new HashCodeBuilder(19, 81);
        hcb.append(fieldData);
        return hcb.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        for (Entry<String, Object> entry : fieldData.entrySet()) {
            tsb.append(entry.getKey(), entry.getValue());
        }
        return tsb.toString();
    }
}

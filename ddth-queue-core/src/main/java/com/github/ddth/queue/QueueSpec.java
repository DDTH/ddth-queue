package com.github.ddth.queue;

import java.util.Map.Entry;
import java.util.Optional;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.github.ddth.commons.utils.DPathUtils;

public class QueueSpec {

    public final String name;

    public final static int NO_BOUNDARY = -1;

    private SortedMap<String, Object> fieldData = new TreeMap<String, Object>();
    public final static String FIELD_EPHEMERAL_DISABLED = "ephemeral_disabled";
    public final static String FIELD_MAX_SIZE = "max_size";
    public final static String FIELD_EPHEMERAL_MAX_SIZE = "ephemeral_max_size";

    public QueueSpec() {
        this(null);
    }

    public QueueSpec(String name) {
        this(name, false, NO_BOUNDARY, NO_BOUNDARY);
    }

    public QueueSpec(boolean ephemeralDisabled) {
        this(null, ephemeralDisabled);
    }

    public QueueSpec(String name, boolean ephemeralDisabled) {
        this(name, ephemeralDisabled, NO_BOUNDARY, NO_BOUNDARY);
    }

    public QueueSpec(int maxSize) {
        this(null, maxSize);
    }

    public QueueSpec(String name, int maxSize) {
        this(name, false, maxSize, NO_BOUNDARY);
    }

    public QueueSpec(int maxSize, int ephemeralMaxSize) {
        this(null, maxSize, ephemeralMaxSize);
    }

    public QueueSpec(String name, int maxSize, int ephemeralMaxSize) {
        this(name, false, maxSize, ephemeralMaxSize);
    }

    public QueueSpec(boolean ephemeralDisabled, int maxSize) {
        this(null, false, maxSize);
    }

    public QueueSpec(String name, boolean ephemeralDisabled, int maxSize) {
        this(name, false, maxSize, NO_BOUNDARY);
    }

    public QueueSpec(boolean ephemeralDisabled, int maxSize, int ephemeralMaxSize) {
        this(null, ephemeralDisabled, maxSize, ephemeralMaxSize);
    }

    public QueueSpec(String name, boolean ephemeralDisabled, int maxSize, int ephemeralMaxSize) {
        this.name = name;
        setField(FIELD_EPHEMERAL_DISABLED, ephemeralDisabled);
        setField(FIELD_MAX_SIZE, maxSize);
        setField(FIELD_EPHEMERAL_MAX_SIZE, ephemeralMaxSize);
    }

    /**
     * Set a field's value.
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
     * Get a field's value.
     * 
     * @param fieldName
     * @param clazz
     * @return
     */
    public <T> T getField(String fieldName, Class<T> clazz) {
        return DPathUtils.getValue(fieldData, fieldName, clazz);
    }

    /**
     * Get a field's value.
     * 
     * @param fieldName
     * @param clazz
     * @return
     */
    public <T> Optional<T> getFieldOptional(String fieldName, Class<T> clazz) {
        return DPathUtils.getValueOptional(fieldData, fieldName, clazz);
    }

    /**
     * Get a field's value.
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
            QueueSpec other = (QueueSpec) obj;
            EqualsBuilder eq = new EqualsBuilder();
            eq.append(name, other.name).append(fieldData, other.fieldData);
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
        hcb.append(name).append(fieldData);
        return hcb.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        ToStringBuilder tsb = new ToStringBuilder(this);
        tsb.append("name", name);
        for (Entry<String, Object> entry : fieldData.entrySet()) {
            tsb.append(entry.getKey(), entry.getValue());
        }
        return tsb.toString();
    }
}

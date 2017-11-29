package com.github.ddth.queue.impl;

/**
 * Abstract ephemeral-support queue implementation.
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.5.0
 */
public abstract class AbstractEphemeralSupportQueue<ID, DATA> extends AbstractQueue<ID, DATA> {

    private boolean ephemeralDisabled = false;
    private int ephemeralMaxSize = 0;

    /**
     * Is ephemeral storage disabled?
     * 
     * @return
     */
    public boolean getEphemeralDisabled() {
        return ephemeralDisabled;
    }

    /**
     * Is ephemeral storage disabled?
     * 
     * @return
     */
    public boolean isEphemeralDisabled() {
        return ephemeralDisabled;
    }

    /**
     * Disables/Enables ephemeral storage.
     * 
     * @param ephemeralDisabled
     *            {@code true} to disable ephemeral storage, {@code false}
     *            otherwise.
     * @return
     */
    public AbstractEphemeralSupportQueue<ID, DATA> setEphemeralDisabled(boolean ephemeralDisabled) {
        this.ephemeralDisabled = ephemeralDisabled;
        return this;
    }

    /**
     * Returns a positive integer as max number of items can be stored in the
     * ephemeral storage.
     * 
     * @return
     */
    public int getEphemeralMaxSize() {
        return ephemeralMaxSize;
    }

    /**
     * Provides a positive integer to limit size (max number of items) of
     * ephemeral storage.
     * 
     * @param ephemeralMaxSize
     * @return
     */
    public AbstractEphemeralSupportQueue<ID, DATA> setEphemeralMaxSize(int ephemeralMaxSize) {
        this.ephemeralMaxSize = ephemeralMaxSize;
        return this;
    }

}

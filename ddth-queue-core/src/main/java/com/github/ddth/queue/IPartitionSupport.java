package com.github.ddth.queue;

/**
 * For queue message partitioning support.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.5.0
 */
public interface IPartitionSupport extends Cloneable {
    /**
     * Key used for partitioning messages (some queue implementations, such as
     * Kafka queue) support message partitioning.
     * 
     * @return
     */
    public String qPartitionKey();

    /**
     * Key used for partitioning messages (some queue implementations, such as
     * Kafka queue) support message partitioning.
     * 
     * @param partitionKey
     * @return
     */
    public IPartitionSupport qPartitionKey(String partitionKey);
}

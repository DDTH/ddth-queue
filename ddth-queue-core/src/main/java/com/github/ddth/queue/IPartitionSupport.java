package com.github.ddth.queue;

/**
 * For (queue) message partitioning support.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.5.0
 */
public interface IPartitionSupport {
    /**
     * Key used for partitioning messages (some queue implementations, such as
     * Kafka queue) support message partitioning.
     * 
     * @return
     * @deprecated since v0.7.0 use {@link #getPartitionKey()}.
     */
    String qPartitionKey();

    /**
     * Key used for partitioning messages (some queue implementations, such as
     * Kafka queue) support message partitioning.
     * 
     * @param partitionKey
     * @return
     * @deprecated since v0.7.0 use {@link #setPartitionKey(String)}.
     */
    IPartitionSupport qPartitionKey(String partitionKey);

    /**
     * Key used for partitioning messages (some queue implementations, such as
     * Kafka queue) support message partitioning.
     * 
     * @return
     * @since 0.7.0
     */
    String getPartitionKey();

    /**
     * Key used for partitioning messages (some queue implementations, such as
     * Kafka queue) support message partitioning.
     * 
     * @param partitionKey
     * @return
     * @since 0.7.0
     */
    IPartitionSupport setPartitionKey(String partitionKey);
}

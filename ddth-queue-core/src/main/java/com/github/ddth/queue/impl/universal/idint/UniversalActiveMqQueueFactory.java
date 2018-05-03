package com.github.ddth.queue.impl.universal.idint;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.ActiveMqQueueFactory;

/**
 * Factory to create {@link UniversalActiveMqQueue} instances.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.6.1
 */
public class UniversalActiveMqQueueFactory
        extends ActiveMqQueueFactory<UniversalActiveMqQueue, Long, byte[]> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalActiveMqQueue createQueueInstance(final QueueSpec spec) {
        UniversalActiveMqQueue queue = new UniversalActiveMqQueue() {
            // private boolean destroyed = false;
            //
            // public void destroy() {
            // if (!destroyed) {
            // destroyed = true;
            // disposeQueue(spec, this);
            // super.destroy();
            // }
            // }
        };
        return queue;
    }

}

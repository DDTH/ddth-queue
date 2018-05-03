package com.github.ddth.queue.impl.universal.idstr;

import com.github.ddth.queue.QueueSpec;
import com.github.ddth.queue.impl.RocksDbQueueFactory;

/**
 * Factory to create {@link UniversalRocksDbQueue} instances.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.4.1
 */
public class UniversalRocksDbQueueFactory
        extends RocksDbQueueFactory<UniversalRocksDbQueue, String, byte[]> {

    /**
     * {@inheritDoc}
     */
    @Override
    protected UniversalRocksDbQueue createQueueInstance(final QueueSpec spec) {
        UniversalRocksDbQueue queue = new UniversalRocksDbQueue() {
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

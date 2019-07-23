package com.github.ddth.queue.internal.utils;

import com.github.ddth.commons.redis.JedisConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Internal Redis utility class.
 *
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 1.0.0
 */
public class RedisUtils {
    private final static Logger LOGGER = LoggerFactory.getLogger(RedisUtils.class);

    /**
     * Close the supplied {@link JedisConnector}.
     *
     * @param jc
     * @param canClose
     * @return
     */
    public static JedisConnector closeJedisConnector(JedisConnector jc, boolean canClose) {
        if (!canClose) {
            return jc;
        }
        if (jc != null) {
            try {
                jc.close();
            } catch (Exception e) {
                LOGGER.warn(e.getMessage(), e);
            }
        }
        return null;
    }
}

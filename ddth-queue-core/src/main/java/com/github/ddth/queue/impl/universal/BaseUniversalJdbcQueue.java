package com.github.ddth.queue.impl.universal;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.JdbcQueue;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Base class for universal JDBC queue implementations.
 *
 * <p>
 * {@link #markFifo(boolean) FIFO}: if {@code true}, queue message with lower id is ensured to be taken first; if {@code false}, order of taken queue messages depends on the DBMS (usually FIFO in most cases).
 * </p>
 *
 * @param <T>
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.6.0
 */
public abstract class BaseUniversalJdbcQueue<T extends BaseUniversalQueueMessage<ID>, ID>
        extends JdbcQueue<ID, byte[]> {
    private boolean fifo = true;

    /**
     * When set to {@code true}, queue message with lower id is ensured to be
     * taken first. When set to {@code false}, order of taken queue messages
     * depends on the DBMS (usually FIFO in most cases).
     *
     * @param fifo
     * @return
     */
    public BaseUniversalJdbcQueue<T, ID> markFifo(boolean fifo) {
        this.fifo = fifo;
        return this;
    }

    /**
     * If {@code true}, queue message with lower id is ensured to be taken
     * first. Otherwise, order of taken queue messages depends on the DBMS
     * (usually FIFO in most cases).
     *
     * @return
     */
    public boolean isFifo() {
        return fifo;
    }

    /**
     * Alias of {@link #markFifo(boolean)}.
     *
     * @param fifo
     * @return
     */
    public BaseUniversalJdbcQueue<T, ID> setFifo(boolean fifo) {
        return markFifo(fifo);
    }

    /**
     * Alias of {@link #isFifo()}.
     *
     * @return
     */
    public boolean getFifo() {
        return isFifo();
    }

    /**
     * Throw {@link IllegalArgumentException} if {@code msg} argument is not of type specified by {@code msgClass}.
     *
     * @param msg
     * @param msgClass
     * @param <X>
     * @return the {@code msg} casted to {@code X}
     * @since 1.0.0
     */
    @SuppressWarnings("unchecked")
    protected <X> X ensureMessageType(IQueueMessage<ID, byte[]> msg, Class<X> msgClass) {
        if (!msgClass.isAssignableFrom(msg.getClass())) {
            throw new IllegalArgumentException(
                    "Expect message argument of type [" + msgClass.getName() + "], but received [" + msg.getClass()
                            .getName() + "]");
        }
        return (X) msg;
    }

    /**
     * Create a message object from input data.
     *
     * @param data
     * @return
     * @since 1.0.0
     */
    @SuppressWarnings("unchecked")
    protected T createMessge(Map<String, Object> data) {
        return (T) (data != null ? ((T) super.createMessage()).fromMap(data) : null);
    }

    /**
     * Convenient method to query list of messages.
     *
     * @param conn
     * @param sql
     * @param params
     * @return
     * @since 1.0.0
     */
    protected List<T> selectMessages(Connection conn, String sql, Map<String, Object> params) {
        List<T> result = new ArrayList<>();
        try (Stream<Map<String, Object>> dbRows = getJdbcHelper().executeSelectAsStream(conn, sql, params)) {
            dbRows.forEach(row -> result.add(createMessge(row)));
        }
        return result;
    }

    /**
     * {@inheritDoc}
     */
    @SuppressWarnings("unchecked")
    @Override
    public T take() {
        return (T) super.take();
    }
}

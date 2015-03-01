package con.github.ddth.queue.utils;

/**
 * Thrown to indicate that there has been an error with queue oeration.
 * 
 * @author Thanh Ba Nguyen <bnguyen2k@gmail.com>
 * @since 0.1.0
 */
public class QueueException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public QueueException() {
    }

    public QueueException(String message) {
        super(message);
    }

    public QueueException(Throwable cause) {
        super(cause);
    }

    public QueueException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueueException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}

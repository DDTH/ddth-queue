package com.github.ddth.queue.utils;

import java.text.MessageFormat;

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

    /*----------------------------------------------------------------------*/

    /**
     * Thrown to indicate that the queue implementation does not support the
     * invoked operation.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.5.0
     */
    public static class OperationNotSupported extends QueueException {
        private static final long serialVersionUID = 1L;

        public OperationNotSupported() {
        }

        public OperationNotSupported(String msg) {
            super(msg);
        }
    }

    /**
     * Thrown to indicate that the queue storage is full.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.5.0
     */
    public static class QueueIsFull extends QueueException {
        private static final long serialVersionUID = 1L;

        public QueueIsFull(int maxSize) {
            super(MessageFormat.format("Queue storage is full (max size: {0})!", maxSize));
        }
    }

    /**
     * Thrown to indicate that the ephemeral storage is full.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.5.0
     */
    public static class EphemeralIsFull extends QueueException {
        private static final long serialVersionUID = 1L;

        public EphemeralIsFull(int maxSize) {
            super(MessageFormat.format("Ephemeral storage is full (max size: {0})!", maxSize));
        }
    }

    /**
     * Thrown to indicate that the queue message can not be serialized.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.3.3
     */
    public static class CannotSerializeQueueMessage extends QueueException {
        private static final long serialVersionUID = 1L;

        public CannotSerializeQueueMessage() {
        }

        public CannotSerializeQueueMessage(String message) {
            super(message);
        }

        public CannotSerializeQueueMessage(Throwable cause) {
            super(cause);
        }

        public CannotSerializeQueueMessage(String message, Throwable cause) {
            super(message, cause);
        }

        public CannotSerializeQueueMessage(String message, Throwable cause,
                boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }

    /**
     * Thrown to indicate that the queue message can not be deserialized.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.3.3
     */
    public static class CannotDeserializeQueueMessage extends QueueException {
        private static final long serialVersionUID = 1L;

        public CannotDeserializeQueueMessage() {
        }

        public CannotDeserializeQueueMessage(String message) {
            super(message);
        }

        public CannotDeserializeQueueMessage(Throwable cause) {
            super(cause);
        }

        public CannotDeserializeQueueMessage(String message, Throwable cause) {
            super(message, cause);
        }

        public CannotDeserializeQueueMessage(String message, Throwable cause,
                boolean enableSuppression, boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }
    }
}

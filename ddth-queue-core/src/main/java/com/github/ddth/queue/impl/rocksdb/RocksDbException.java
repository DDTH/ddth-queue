package com.github.ddth.queue.impl.rocksdb;

/**
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * @since 0.5.0
 */
public class RocksDbException extends RuntimeException {

    /**
     * Thrown to indicate the specified column family does not exist.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.5.0
     */
    public static class ColumnFamilyNotExists extends RocksDbException {
        private static final long serialVersionUID = 1L;

        public ColumnFamilyNotExists(String cfName) {
            super("Column family [" + cfName + "] does not exist!");
        }
    }

    /**
     * Thrown to indicate the write/delete operation is not permitted because
     * the DB is opened in read-only mode.
     * 
     * @author Thanh Nguyen <btnguyen2k@gmail.com>
     * @since 0.5.0
     */
    public static class ReadOnlyException extends RocksDbException {
        private static final long serialVersionUID = 1L;

        public ReadOnlyException() {
            super("Modification operation is not permitted because the DB is opened in read-only mode!");
        }
    }

    private static final long serialVersionUID = 1L;

    public RocksDbException() {
    }

    public RocksDbException(String message) {
        super(message);
    }

    public RocksDbException(Throwable cause) {
        super(cause);
    }

    public RocksDbException(String message, Throwable cause) {
        super(message, cause);
    }

    public RocksDbException(String message, Throwable cause, boolean enableSuppression,
            boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }

}

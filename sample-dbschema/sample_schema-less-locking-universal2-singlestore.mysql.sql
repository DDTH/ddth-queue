-- Sample table schema for less-locking universal2 MySQL queue

DROP TABLE IF EXISTS queuell2;
CREATE TABLE queuell2 (
    queue_name                  VARCHAR(64)         NOT NULL            COMMENT "Queue's name, messages of multiple queues can be store in same db table",
        INDEX (queue_name),
    queue_id                    VARCHAR(32)         NOT NULL,
        PRIMARY KEY (queue_id),
    ephemeral_id                VARCHAR(32),
        INDEX (ephemeral_id),
    msg_org_timestamp           DATETIME            NOT NULL            COMMENT "Message's original timestamp; when requeued original timestamp will not be changed",
    msg_timestamp               DATETIME            NOT NULL            COMMENT "Message's queue timestamp",
        INDEX (msg_timestamp),
    msg_num_requeues            INT                 NOT NULL DEFAULT 0  COMMENT "How many times message has been requeued",
    msg_content                 BLOB                                    COMMENT "Message's content"
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

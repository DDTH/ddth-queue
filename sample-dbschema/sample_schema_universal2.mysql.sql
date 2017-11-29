-- Sample table schema for universal2 MySQL queue

DROP TABLE IF EXISTS queue2;
CREATE TABLE queue2 (
    queue_id                    VARCHAR(32)         NOT NULL,
        PRIMARY KEY (queue_id),
    msg_org_timestamp           DATETIME            NOT NULL            COMMENT "Message's original timestamp; when requeued original timestamp will not be changed",
    msg_timestamp               DATETIME            NOT NULL            COMMENT "Message's queue timestamp",
    msg_num_requeues            INT                 NOT NULL DEFAULT 0  COMMENT "How many times message has been requeued",
    msg_content                 BLOB                                    COMMENT "Message's content"
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

DROP TABLE IF EXISTS queue2_ephemeral;
CREATE TABLE queue2_ephemeral (
    queue_id                    VARCHAR(32)         NOT NULL,
        PRIMARY KEY (queue_id),
    msg_org_timestamp           DATETIME            NOT NULL,
    msg_timestamp               DATETIME            NOT NULL,
        INDEX (msg_timestamp),
    msg_num_requeues            INT                 NOT NULL DEFAULT 0,
    msg_content                 BLOB
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
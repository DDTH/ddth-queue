-- Sample table schema for universal, single-store MySQL queue

DROP TABLE IF EXISTS queuess;
CREATE TABLE queuess (
    queue_name                  VARCHAR(64)         NOT NULL            COMMENT "Queue's name, messages of multiple queues can be store in same db table",
        INDEX (queue_name),
    queue_id                    BIGINT              AUTO_INCREMENT,
        PRIMARY KEY (queue_id),
    msg_org_timestamp           DATETIME            NOT NULL            COMMENT "Message's original timestamp; when requeued original timestamp will not be changed",
    msg_timestamp               DATETIME            NOT NULL            COMMENT "Message's queue timestamp",
    msg_num_requeues            INT                 NOT NULL DEFAULT 0  COMMENT "How many times message has been requeued",
    msg_content                 BLOB                                    COMMENT "Message's content"
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

DROP TABLE IF EXISTS queuess_ephemeral;
CREATE TABLE queuess_ephemeral (
    queue_name                  VARCHAR(64)         NOT NULL,
        INDEX (queue_name),
    queue_id                    BIGINT              NOT NULL,
        PRIMARY KEY (queue_id),
    msg_org_timestamp           DATETIME            NOT NULL,
    msg_timestamp               DATETIME            NOT NULL,
        INDEX (msg_timestamp),
    msg_num_requeues            INT                 NOT NULL DEFAULT 0,
    msg_content                 BLOB
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;

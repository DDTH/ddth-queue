DROP TABLE IF EXISTS queue;
CREATE TABLE queue (
    queue_id                    BIGINT              AUTO_INCREMENT,
        PRIMARY KEY (queue_id),
    msg_org_timestamp           DATETIME            NOT NULL            COMMENT "Message's original timestamp, when requeued original timestamp unchanged",
    msg_timestamp               DATETIME            NOT NULL            COMMENT "Message's queue timestamp",
    msg_num_requeues            INT                 NOT NULL DEFAULT 0  COMMENT "How many times message has been requeued",
    msg_content                 VARCHAR(255)        NOT NULL            COMMENT "MO's full content, including the command code"
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;

DROP TABLE IF EXISTS queue_ephemeral;
CREATE TABLE queue_ephemeral (
    queue_id                    BIGINT              NOT NULL,
        PRIMARY KEY (queue_id),
    msg_org_timestamp           DATETIME            NOT NULL            COMMENT "Message's original timestamp, when requeued original timestamp unchanged",
    msg_timestamp               DATETIME            NOT NULL            COMMENT "Message's queue timestamp",
    msg_num_requeues            INT                 NOT NULL DEFAULT 0  COMMENT "How many times message has been requeued",
    msg_content                 VARCHAR(255)        NOT NULL            COMMENT "MO's full content, including the command code"
) ENGINE=InnoDB DEFAULT CHARACTER SET utf8 COLLATE utf8_unicode_ci;

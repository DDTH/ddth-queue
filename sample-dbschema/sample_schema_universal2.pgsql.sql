-- Sample table schema for universal2 PgSQL queue

DROP TABLE IF EXISTS queue2;
CREATE TABLE queue2 (
    queue_id                    VARCHAR(32)         NOT NULL,
        PRIMARY KEY (queue_id),
    msg_org_timestamp           TIMESTAMP           NOT NULL,
    msg_timestamp               TIMESTAMP           NOT NULL,
    msg_num_requeues            INT                 NOT NULL DEFAULT 0,
    msg_content                 BYTEA
);

DROP TABLE IF EXISTS queue_ephemeral2;
CREATE TABLE queue_ephemeral2 (
    queue_id                    VARCHAR(32)         NOT NULL,
        PRIMARY KEY (queue_id),
    msg_org_timestamp           TIMESTAMP           NOT NULL,
    msg_timestamp               TIMESTAMP           NOT NULL,
    msg_num_requeues            INT                 NOT NULL DEFAULT 0,
    msg_content                 BYTEA
);
CREATE INDEX queue_ephemeral2_msg_timestamp ON queue_ephemeral2(msg_timestamp);

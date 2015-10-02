-- Sample table schema for universal2 PgSQL queue

DROP TABLE IF EXISTS queue;
CREATE TABLE queue (
    queue_id                    VARCHAR(32)         NOT NULL,
        PRIMARY KEY (queue_id),
    msg_org_timestamp           TIMESTAMP           NOT NULL,
    msg_timestamp               TIMESTAMP           NOT NULL,
    msg_num_requeues            INT                 NOT NULL DEFAULT 0,
    msg_content                 BYTEA
);

DROP TABLE IF EXISTS queue_ephemeral;
CREATE TABLE queue_ephemeral (
    queue_id                    VARCHAR(32)         NOT NULL,
        PRIMARY KEY (queue_id),
    msg_org_timestamp           TIMESTAMP           NOT NULL,
    msg_timestamp               TIMESTAMP           NOT NULL,
    msg_num_requeues            INT                 NOT NULL DEFAULT 0,
    msg_content                 BYTEA
);
CREATE INDEX queue_ephemeral_msg_timestamp ON queue_ephemeral(msg_timestamp);

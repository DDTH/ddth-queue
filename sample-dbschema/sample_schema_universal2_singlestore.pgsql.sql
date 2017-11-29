-- Sample table schema for universal2, single-store PgSQL queue

DROP TABLE IF EXISTS queue2ss;
CREATE TABLE queue2ss (
    queue_name                  VARCHAR(64)         NOT NULL,
    queue_id                    VARCHAR(32)         NOT NULL,
        PRIMARY KEY (queue_id),
    msg_org_timestamp           TIMESTAMP           NOT NULL,
    msg_timestamp               TIMESTAMP           NOT NULL,
    msg_num_requeues            INT                 NOT NULL DEFAULT 0,
    msg_content                 BYTEA
);
CREATE INDEX queue2ss_queue_name ON queue2ss(queue_name);

DROP TABLE IF EXISTS queue2ss_ephemeral;
CREATE TABLE queue2ss_ephemeral (
    queue_name                  VARCHAR(64)         NOT NULL,
    queue_id                    VARCHAR(32)         NOT NULL,
        PRIMARY KEY (queue_id),
    msg_org_timestamp           TIMESTAMP           NOT NULL,
    msg_timestamp               TIMESTAMP           NOT NULL,
    msg_num_requeues            INT                 NOT NULL DEFAULT 0,
    msg_content                 BYTEA
);
CREATE INDEX queue2ss_ephemeral_queue_name ON queue2ss_ephemeral(queue_name);
CREATE INDEX queue2ss_ephemeral_msg_timestamp ON queue2ss_ephemeral(msg_timestamp);

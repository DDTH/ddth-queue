-- Sample table schema for less-locking universal2 PgSQL queue

DROP TABLE IF EXISTS queuell2;
CREATE TABLE queuell2 (
    queue_id                    VARCHAR(32)         NOT NULL,
        PRIMARY KEY (queue_id),
    ephemeral_id                VARCHAR(32),
    msg_org_timestamp           TIMESTAMP           NOT NULL,
    msg_timestamp               TIMESTAMP           NOT NULL,
    msg_num_requeues            INT                 NOT NULL DEFAULT 0,
    msg_content                 BYTEA
);
CREATE INDEX queuell2_ephemeral_id ON queuell2(ephemeral_id);
CREATE INDEX queuell2_msg_timestamp ON queuell2(msg_timestamp);

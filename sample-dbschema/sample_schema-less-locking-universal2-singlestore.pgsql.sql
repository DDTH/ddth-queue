-- Sample table schema for less-locking universal2 PgSQL queue

DROP TABLE IF EXISTS queuell2ss;
CREATE TABLE queuell2ss (
    queue_name                  VARCHAR(64)         NOT NULL,
    queue_id                    VARCHAR(32)         NOT NULL,
        PRIMARY KEY (queue_id),
    ephemeral_id                VARCHAR(32),
    msg_org_timestamp           TIMESTAMP           NOT NULL,
    msg_timestamp               TIMESTAMP           NOT NULL,
    msg_num_requeues            INT                 NOT NULL DEFAULT 0,
    msg_content                 BYTEA
);
CREATE INDEX queuell2ss_queue_name ON queuell2ss(queue_name);
CREATE INDEX queuell2ss_ephemeral_id ON queuell2ss(ephemeral_id);
CREATE INDEX queuell2ss_msg_timestamp ON queuell2ss(msg_timestamp);

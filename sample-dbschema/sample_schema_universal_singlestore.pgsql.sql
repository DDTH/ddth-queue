-- Sample table schema for universal, single-store PgSQL queue

DROP TABLE IF EXISTS queuess;
CREATE TABLE queuess (
    queue_name                  VARCHAR(64)         NOT NULL,
    queue_id                    BIGSERIAL,
        PRIMARY KEY (queue_id),
    msg_org_timestamp           TIMESTAMP           NOT NULL,
    msg_timestamp               TIMESTAMP           NOT NULL,
    msg_num_requeues            INT                 NOT NULL DEFAULT 0,
    msg_content                 BYTEA
);
CREATE INDEX queuess_queue_name ON queuess(queue_name);

DROP TABLE IF EXISTS queuess_ephemeral;
CREATE TABLE queuess_ephemeral (
    queue_name                  VARCHAR(64)         NOT NULL,
    queue_id                    BIGINT              NOT NULL,
        PRIMARY KEY (queue_id),
    msg_org_timestamp           TIMESTAMP           NOT NULL,
    msg_timestamp               TIMESTAMP           NOT NULL,
    msg_num_requeues            INT                 NOT NULL DEFAULT 0,
    msg_content                 BYTEA
);
CREATE INDEX queuess_ephemeral_queue_name ON queuess_ephemeral(queue_name);
CREATE INDEX queuess_ephemeral_msg_timestamp ON queuess_ephemeral(msg_timestamp);

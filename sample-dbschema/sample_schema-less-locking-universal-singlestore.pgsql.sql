-- Sample table schema for less-locking universal PgSQL queue

--CREATE USER test PASSWORD 'test';
--CREATE DATABASE temp OWNER='test' TEMPLATE='template0';

DROP TABLE IF EXISTS queuellss;
CREATE TABLE queuellss (
    queue_name                  VARCHAR(64)         NOT NULL,
    queue_id                    BIGSERIAL,
        PRIMARY KEY (queue_id),
    ephemeral_id                BIGINT              NOT NULL DEFAULT 0,
    msg_org_timestamp           TIMESTAMP           NOT NULL,
    msg_timestamp               TIMESTAMP           NOT NULL,
    msg_num_requeues            INT                 NOT NULL DEFAULT 0,
    msg_content                 BYTEA
);
CREATE INDEX queuellss_queue_name ON queuellss(queue_name);
CREATE INDEX queuellss_ephemeral_id ON queuellss(ephemeral_id);
CREATE INDEX queuellss_msg_timestamp ON queuellss(msg_timestamp);

-- Sample table schema for less-locking universal PgSQL queue

CREATE USER test PASSWORD 'test';
CREATE DATABASE temp OWNER='test' TEMPLATE='template0';

DROP TABLE IF EXISTS queuell;
CREATE TABLE queuell (
    queue_id                    BIGSERIAL,
        PRIMARY KEY (queue_id),
    ephemeral_id                BIGINT              NOT NULL DEFAULT 0,
    msg_org_timestamp           TIMESTAMP           NOT NULL,
    msg_timestamp               TIMESTAMP           NOT NULL,
    msg_num_requeues            INT                 NOT NULL DEFAULT 0,
    msg_content                 BYTEA
);
CREATE INDEX queuell_ephemeral_id ON queuell(ephemeral_id);
CREATE INDEX queuell_msg_timestamp ON queuell(msg_timestamp);

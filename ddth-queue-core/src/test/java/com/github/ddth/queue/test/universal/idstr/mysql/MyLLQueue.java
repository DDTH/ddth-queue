package com.github.ddth.queue.test.universal.idstr.mysql;

import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.queue.impl.universal.idstr.LessLockingUniversalMySQLQueue;

public class MyLLQueue extends LessLockingUniversalMySQLQueue {
    public MyLLQueue init() throws Exception {
        super.init();
        {
            IJdbcHelper jdbcHelper = getJdbcHelper();
            jdbcHelper.execute("DROP TABLE IF EXISTS " + getTableName());
            jdbcHelper.execute("CREATE TABLE " + getTableName() + "(\n"
                    + "    queue_id                    VARCHAR(32)         NOT NULL,\n"
                    + "        PRIMARY KEY (queue_id),\n"
                    + "    ephemeral_id                BIGINT              NOT NULL DEFAULT 0,\n"
                    + "        INDEX (ephemeral_id),\n"
                    + "    msg_org_timestamp           DATETIME            NOT NULL            COMMENT \"Message's original timestamp; when requeued original timestamp will not be changed\",\n"
                    + "    msg_timestamp               DATETIME            NOT NULL            COMMENT \"Message's queue timestamp\",\n"
                    + "        INDEX (msg_timestamp),\n"
                    + "    msg_num_requeues            INT                 NOT NULL DEFAULT 0  COMMENT \"How many times message has been requeued\",\n"
                    + "    msg_content                 BLOB                                    COMMENT \"Message's content\"\n"
                    + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
        }
        return this;
    }

    public void flush() {
        IJdbcHelper jdbcHelper = getJdbcHelper();
        jdbcHelper.execute("DELETE FROM " + getTableName());
    }
}

package com.github.ddth.queue.test.universal.idint.mysql;

import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.queue.impl.universal.idint.LessLockingUniversalSingleStorageMySQLQueue;

public class MyLLSSQueue extends LessLockingUniversalSingleStorageMySQLQueue {
    public MyLLSSQueue init() throws Exception {
        super.init();
        {
            IJdbcHelper jdbcHelper = getJdbcHelper();
            jdbcHelper.execute("DROP TABLE IF EXISTS " + getTableName());
            jdbcHelper.execute("CREATE TABLE " + getTableName() + "(\n"
                    + "    queue_name                  VARCHAR(64)         NOT NULL            COMMENT \"Queue's name, messages of multiple queues can be store in same db table\",\n"
                    + "        INDEX (queue_name),\n"
                    + "    queue_id                    BIGINT              AUTO_INCREMENT,\n"
                    + "        PRIMARY KEY (queue_id),\n"
                    + "    ephemeral_id                BIGINT              NOT NULL DEFAULT 0,\n"
                    + "    msg_org_timestamp           DATETIME            NOT NULL            COMMENT \"Message's original timestamp; when requeued original timestamp will not be changed\",\n"
                    + "    msg_timestamp               DATETIME            NOT NULL            COMMENT \"Message's queue timestamp\",\n"
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

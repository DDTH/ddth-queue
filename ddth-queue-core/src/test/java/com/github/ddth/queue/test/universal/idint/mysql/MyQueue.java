package com.github.ddth.queue.test.universal.idint.mysql;

import com.github.ddth.dao.jdbc.IJdbcHelper;
import com.github.ddth.queue.impl.universal.idint.UniversalJdbcQueue;

public class MyQueue extends UniversalJdbcQueue {
    public MyQueue init() throws Exception {
        super.init();
        {
            IJdbcHelper jdbcHelper = getJdbcHelper();
            if (getTableName() != null) {
                jdbcHelper.execute("DROP TABLE IF EXISTS " + getTableName());
                jdbcHelper.execute("CREATE TABLE " + getTableName() + "(\n"
                        + "    queue_id                    BIGINT              AUTO_INCREMENT,\n"
                        + "        PRIMARY KEY (queue_id),\n"
                        + "    msg_org_timestamp           DATETIME            NOT NULL            COMMENT \"Message's original timestamp; when requeued original timestamp will not be changed\",\n"
                        + "    msg_timestamp               DATETIME            NOT NULL            COMMENT \"Message's queue timestamp\",\n"
                        + "    msg_num_requeues            INT                 NOT NULL DEFAULT 0  COMMENT \"How many times message has been requeued\",\n"
                        + "    msg_content                 BLOB                                    COMMENT \"Message's content\"\n"
                        + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
            }
            if (getTableNameEphemeral() != null) {
                jdbcHelper.execute("DROP TABLE IF EXISTS " + getTableNameEphemeral());
                jdbcHelper.execute("CREATE TABLE " + getTableNameEphemeral() + "(\n"
                        + "    queue_id                    BIGINT              NOT NULL,\n"
                        + "        PRIMARY KEY (queue_id),\n"
                        + "    msg_org_timestamp           DATETIME            NOT NULL,\n"
                        + "    msg_timestamp               DATETIME            NOT NULL,\n"
                        + "        INDEX (msg_timestamp),\n"
                        + "    msg_num_requeues            INT                 NOT NULL DEFAULT 0,\n"
                        + "    msg_content                 BLOB\n"
                        + ") ENGINE=InnoDB DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci");
            }
        }
        return this;
    }

    public void flush() {
        IJdbcHelper jdbcHelper = getJdbcHelper();
        if (getTableName() != null) {
            jdbcHelper.execute("DELETE FROM " + getTableName());
        }
        if (getTableNameEphemeral() != null) {
            jdbcHelper.execute("DELETE FROM " + getTableNameEphemeral());
        }
    }
}

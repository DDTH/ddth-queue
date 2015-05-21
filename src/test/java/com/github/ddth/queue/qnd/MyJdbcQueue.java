package com.github.ddth.queue.qnd;

import java.text.MessageFormat;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.JdbcTemplate;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.JdbcQueue;

public class MyJdbcQueue extends JdbcQueue {

    public MyJdbcQueue() {
        setTableName("queue");
        setTableNameEphemeral("queue_ephemeral");
    }

    @Override
    protected IQueueMessage readFromQueueStorage(JdbcTemplate jdbcTemplate) {
        final String SQL = "SELECT * FROM {0} ORDER BY queue_id LIMIT 1";
        List<Map<String, Object>> dbRows = jdbcTemplate.queryForList(MessageFormat.format(SQL,
                getTableName()));
        if (dbRows != null && dbRows.size() > 0) {
            Map<String, Object> dbRow = dbRows.get(0);
            MyQueueMessage msg = new MyQueueMessage();
            return (IQueueMessage) msg.fromMap(dbRow);
        }
        return null;
    }

    @Override
    protected IQueueMessage readFromEphemeralStorage(JdbcTemplate jdbcTemplate) {
        final String SQL = "SELECT * FROM {0} ORDER BY queue_id LIMIT 1";
        List<Map<String, Object>> dbRows = jdbcTemplate.queryForList(MessageFormat.format(SQL,
                getTableNameEphemeral()));
        if (dbRows != null && dbRows.size() > 0) {
            Map<String, Object> dbRow = dbRows.get(0);
            MyQueueMessage msg = new MyQueueMessage();
            return (IQueueMessage) msg.fromMap(dbRow);
        }
        return null;
    }

    @Override
    protected boolean putToQueueStorage(JdbcTemplate jdbcTemplate, IQueueMessage _msg) {
        MyQueueMessage msg = (MyQueueMessage) _msg;
        final String SQL = "INSERT INTO {0} (queue_id, msg_org_timestamp, msg_timestamp, msg_num_requeues, msg_content) VALUES (?, ?, ?, ?, ?)";
        int numRows = jdbcTemplate.update(MessageFormat.format(SQL, getTableName()), msg.qId(),
                msg.qOriginalTimestamp(), msg.qTimestamp(), msg.qNumRequeues(), msg.content());
        return numRows > 0;
    }

    @Override
    protected boolean putToEphemeralStorage(JdbcTemplate jdbcTemplate, IQueueMessage _msg) {
        MyQueueMessage msg = (MyQueueMessage) _msg;
        final String SQL = "INSERT INTO {0} (queue_id, msg_org_timestamp, msg_timestamp, msg_num_requeues, msg_content) VALUES (?, ?, ?, ?, ?)";
        int numRows = jdbcTemplate.update(MessageFormat.format(SQL, getTableNameEphemeral()),
                msg.qId(), msg.qOriginalTimestamp(), msg.qTimestamp(), msg.qNumRequeues(),
                msg.content());
        return numRows > 0;
    }

    @Override
    protected boolean removeFromQueueStorage(JdbcTemplate jdbcTemplate, IQueueMessage msg) {
        final String SQL = "DELETE FROM {0} WHERE queue_id=?";
        int numRows = jdbcTemplate.update(MessageFormat.format(SQL, getTableName()), msg.qId());
        return numRows > 0;
    }

    @Override
    protected boolean removeFromEphemeralStorage(JdbcTemplate jdbcTemplate, IQueueMessage msg) {
        final String SQL = "DELETE FROM {0} WHERE queue_id=?";
        int numRows = jdbcTemplate.update(MessageFormat.format(SQL, getTableNameEphemeral()),
                msg.qId());
        return numRows > 0;
    }

}

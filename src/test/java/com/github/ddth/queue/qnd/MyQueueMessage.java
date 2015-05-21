package com.github.ddth.queue.qnd;

import java.util.Date;

import com.github.ddth.dao.BaseBo;
import com.github.ddth.queue.IQueueMessage;

public class MyQueueMessage extends BaseBo implements IQueueMessage {

    @Override
    public Object qId() {
        return getAttribute("queue_id", long.class);
    }

    @Override
    public MyQueueMessage qId(Object queueId) {
        return (MyQueueMessage) setAttribute("queue_id", queueId);
    }

    @Override
    public Date qOriginalTimestamp() {
        return getAttribute("msg_org_timestamp", Date.class);
    }

    @Override
    public MyQueueMessage qOriginalTimestamp(Date timestamp) {
        return (MyQueueMessage) setAttribute("msg_org_timestamp", timestamp);
    }

    @Override
    public Date qTimestamp() {
        return getAttribute("msg_timestamp", Date.class);
    }

    @Override
    public MyQueueMessage qTimestamp(Date timestamp) {
        return (MyQueueMessage) setAttribute("msg_timestamp", timestamp);
    }

    @Override
    public int qNumRequeues() {
        return getAttribute("msg_num_requeues", int.class);
    }

    @Override
    public MyQueueMessage qNumRequeues(int numRequeues) {
        return (MyQueueMessage) setAttribute("msg_num_requeues", numRequeues);
    }

    @Override
    public MyQueueMessage qIncNumRequeues() {
        return qNumRequeues(qNumRequeues() + 1);
    }

    public String content() {
        return getAttribute("msg_content", String.class);
    }

    public MyQueueMessage content(String content) {
        return (MyQueueMessage) setAttribute("msg_content", content);
    }
}

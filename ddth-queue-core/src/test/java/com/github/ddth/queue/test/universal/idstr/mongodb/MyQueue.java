package com.github.ddth.queue.test.universal.idstr.mongodb;

import com.github.ddth.queue.impl.universal.idstr.UniversalMongodbQueue;

public class MyQueue extends UniversalMongodbQueue {
    public void flush() {
        getCollection().drop();
        initCollection();
    }
}

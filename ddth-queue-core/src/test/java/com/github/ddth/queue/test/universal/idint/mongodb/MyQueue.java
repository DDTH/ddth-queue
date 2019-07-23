package com.github.ddth.queue.test.universal.idint.mongodb;

import com.github.ddth.queue.impl.universal.idint.UniversalMongodbQueue;

public class MyQueue extends UniversalMongodbQueue {
    public void flush() {
        getCollection().drop();
        initCollection();
    }
}

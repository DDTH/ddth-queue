package com.github.ddth.queue.test.universal.idstr.kafka;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ 
    TestKafkaQueue.class, TestKafkaQueueLong.class, TestKafkaQueueMT.class
})

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.kafka.MySuiteTest -DenableTestsKafka=true
 */

public class MySuiteTest {
}

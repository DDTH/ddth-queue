package com.github.ddth.queue.test.universal.idstr.rabbitmq;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ 
    TestRabbitMqQueue.class, TestRabbitMqQueueLong.class, TestRabbitMqQueueMT.class
})

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idstr.rabbitmq.MySuiteTest -DenableTestsRabbitMq=true
 */

public class MySuiteTest {
}

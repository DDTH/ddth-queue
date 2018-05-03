package com.github.ddth.queue.test.universal.idint.disruptor;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ TestDisruptorQueue.class, TestDisruptorQueueLarge.class,
        TestDisruptorQueueLargeBoundEphemeralSize.class,
        TestDisruptorQueueLargeEphemeralDisabled.class, TestDisruptorQueueSmall.class,
        TestDisruptorQueueSmallBoundEphemeralSize.class,
        TestDisruptorQueueSmallEphemeralDisabled.class, TestDisruptorQueueXLarge.class,
        TestDisruptorQueueXLargeBoundEphemeralSize.class,
        TestDisruptorQueueXLargeEphemeralDisabled.class, TestDisruptorQueueXSmall.class,
        TestDisruptorQueueXSmallBoundEphemeralSize.class,
        TestDisruptorQueueXSmallEphemeralDisabled.class })

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.disruptor.MySuiteTest
 */

public class MySuiteTest {
}

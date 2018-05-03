package com.github.ddth.queue.test.universal.idint.inmem;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ 
    TestInmemQueue.class,
    TestInmemQueueBoundLarge.class,
    TestInmemQueueBoundLargeBoundEphemeralSize.class,
    TestInmemQueueBoundLargeEphemeralDisabled.class,
    TestInmemQueueBoundSmall.class,
    TestInmemQueueBoundSmallBoundEphemeralSize.class,
    TestInmemQueueBoundSmallEphemeralDisabled.class,
    TestInmemQueueBoundXLarge.class,
    TestInmemQueueBoundXLargeBoundEphemeralSize.class,
    TestInmemQueueBoundXLargeEphemeralDisabled.class,
    TestInmemQueueBoundXSmall.class,
    TestInmemQueueBoundXSmallBoundEphemeralSize.class,
    TestInmemQueueBoundXSmallEphemeralDisabled.class,
    TestInmemQueueUnbound.class,
    TestInmemQueueUnboundBoundEphemeralSize.class,
    TestInmemQueueUnboundEphemeralDisabled.class
})

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.queue.test.universal.idint.inmem.MySuiteTest
 */

public class MySuiteTest {
}

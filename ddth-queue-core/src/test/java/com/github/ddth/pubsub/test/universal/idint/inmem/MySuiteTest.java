package com.github.ddth.pubsub.test.universal.idint.inmem;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ TestInmemPubSubHub.class, TestInmemPubSubMT.class })

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.pubsub.test.universal.idint.inmem.MySuiteTest
 */

public class MySuiteTest {
}

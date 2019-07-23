package com.github.ddth.pubsub.test.universal.idstr.mongodb;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)

@Suite.SuiteClasses({ TestMongodbPubSubHub.class, TestMongodbPubSubMT.class })

/*
 * mvn test -DskipTests=false -Dtest=com.github.ddth.pubsub.test.universal.idstr.mongodb.MySuiteTest -DenableTestsMongo=true
 */

public class MySuiteTest {
}

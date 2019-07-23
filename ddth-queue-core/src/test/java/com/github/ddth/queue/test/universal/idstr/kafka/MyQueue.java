package com.github.ddth.queue.test.universal.idstr.kafka;

import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.idstr.UniversalKafkaQueue;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class MyQueue extends UniversalKafkaQueue {
    public void initTopic(String topic, int numPartitions) throws ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", getKafkaBootstrapServers());
        try (AdminClient adminClient = AdminClient.create(props)) {
            try {
                DeleteTopicsResult result = adminClient.deleteTopics(Arrays.asList(topic));
                result.all().get();
            } catch (ExecutionException e) {
            }
            Thread.sleep(3210);

            NewTopic newTopic = new NewTopic(topic, numPartitions, (short) 1);
            {
                CreateTopicsResult result = adminClient.createTopics(Arrays.asList(newTopic));
                result.all().get();
            }
            Thread.sleep(3456);
        }
    }

    public MyQueue init() throws Exception {
        super.init();
        initTopic(getTopicName(), 1);
        return this;
    }

    public void flush() {
        int numMsgs = 0;
        long t1 = System.currentTimeMillis();
        IQueueMessage<String, byte[]> msg = takeFromQueue();
        while (msg != null) {
            numMsgs++;
            msg = takeFromQueue();
        }
        System.out.println("* Flush " + numMsgs + " msgs from queue in " + (System.currentTimeMillis() - t1) + "ms.");
    }
}

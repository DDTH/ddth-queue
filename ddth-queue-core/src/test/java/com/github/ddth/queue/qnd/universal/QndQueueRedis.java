package com.github.ddth.queue.qnd.universal;

import com.github.ddth.commons.redis.JedisConnector;
import com.github.ddth.queue.IQueueMessage;
import com.github.ddth.queue.impl.universal.UniversalIdIntQueueMessage;
import com.github.ddth.queue.impl.universal.idint.UniversalRedisQueue;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Response;
import redis.clients.jedis.Transaction;

import java.nio.charset.StandardCharsets;
import java.util.Date;

public class QndQueueRedis {
    public static void main(String[] args) throws Exception {

        try (final UniversalRedisQueue queue = new UniversalRedisQueue()) {
            queue.setRedisHostAndPort("localhost:6379").init();

            JedisConnector jc = queue.getJedisConnector();
            try (Jedis jedis = jc.getJedis()) {
                UniversalIdIntQueueMessage orgMsg = UniversalIdIntQueueMessage.newInstance();
                orgMsg.setContent("Content: [" + System.currentTimeMillis() + "] " + new Date());
                System.out.println("Original Message: " + orgMsg);

                byte[] data = queue.getSerDeser().toBytes(orgMsg);
                System.out.println("Serialized: " + data.length);

                try (Transaction jt = jedis.multi()) {
                    String id = orgMsg.getId().toString();
                    System.out.println("ID: " + id);
                    byte[] field = id.getBytes(StandardCharsets.UTF_8);
                    Response<?>[] responses = new Response[] { jt.hset(queue.getRedisHashNameAsBytes(), field, data),
                            jt.rpush(queue.getRedisListNameAsBytes(), field) };
                    jt.exec();
                    for (Response<?> response : responses) {
                        System.out.println("\tresponse: " + response.get());
                    }
                }

                {
                    String id = orgMsg.getId().toString();
                    byte[] field = id.getBytes(StandardCharsets.UTF_8);
                    byte[] redisData = jedis.hget(queue.getRedisHashNameAsBytes(), field);
                    IQueueMessage msg = queue.getSerDeser().fromBytes(redisData, IQueueMessage.class);
                    System.out.println("HGET: " + redisData.length);
                    System.out.println("Message: " + msg);
                }

                {
                    String scriptTake = queue.getScriptTake();
                    scriptTake = "local qid=redis.call(\"lpop\",\"queue_l\");"
                            + " if qid then redis.call(\"zadd\", \"queue_s\", ARGV[1], qid);"
                            + " return redis.call(\"hget\", \"queue_h\", qid);" + " else return nil end";
                    System.out.println("Script Take: " + scriptTake);
                    long now = System.currentTimeMillis();
                    Object response = jedis.eval(scriptTake.getBytes(StandardCharsets.UTF_8), 0,
                            String.valueOf(now).getBytes(StandardCharsets.UTF_8));
                    if (response instanceof byte[]) {
                        byte[] resData = (byte[]) response;
                        System.out.println("Take: " + resData.length + " / " + new String((byte[]) response));
                    } else {
                        byte[] resData = response.toString().getBytes(StandardCharsets.UTF_8);
                        System.out.println("Take: " + resData.length + " / " + response);
                    }
                    //                    System.out.println(new String(data, StandardCharsets.UTF_8));
                    //                    System.out.println("Respsone: " + response.getClass());
                    //                    System.out.println("Respsone: " + response);
                    //                    return response == null ?
                    //                            null :
                    //                            deserialize(response instanceof byte[] ?
                    //                                    (byte[]) response :
                    //                                    response.toString().getBytes(StandardCharsets.UTF_8));
                }
            }

            UniversalIdIntQueueMessage msg = UniversalIdIntQueueMessage.newInstance();
            msg.setContent("Content: [" + System.currentTimeMillis() + "] " + new Date());
            System.out.println("Queue: " + queue.queue(msg));

            msg = queue.take();
            System.out.println("Take: " + msg);
            while (msg.getNumRequeues() < 2) {
                System.out.println("\tMessage: " + msg);
                System.out.println("\tContent: " + new String(msg.getContent()));
                System.out.println("\tRequeue: " + queue.requeue(msg));
                msg = queue.take();
            }
            queue.finish(msg);
        }
    }
}

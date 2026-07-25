package com.processing.logs.processor;

import redis.clients.jedis.Jedis;

public class OffsetGuard {

    private static final String REDIS_HOST = System.getenv().getOrDefault("REDIS_HOST", "10.99.39.122");
    private static final int REDIS_PORT = 6379;

    public static boolean isAlreadyProcessed(String partitionKey, long incomingOffset){
        Jedis jedisConnect = new Jedis(REDIS_HOST, REDIS_PORT);
        try{
        return jedisConnect.sismember(partitionKey, String.valueOf(incomingOffset));
    }
    catch (Exception e){
            throw new RuntimeException("Failed fetching data from Redis ------ ", e);
    }
    finally {
        jedisConnect.close();
    }
}

    public static void markProcessed(String partitionKey, long offset) {
        Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
        try {
            jedis.sadd(partitionKey, String.valueOf(offset));
            jedis.expire(partitionKey, 86400); // this is reset of every new add to the set, so we can keep it for 24 hours
        } catch (Exception e) {
            throw new RuntimeException("Failed adding to Redis set", e);
        } finally {
            jedis.close();
        }
    }

    public static void clearProcessed(String partitionKey, long offset) {
    Jedis jedis = new Jedis(REDIS_HOST, REDIS_PORT);
    try {
        jedis.srem(partitionKey, String.valueOf(offset));
    } catch (Exception e) {
        throw new RuntimeException("Failed removing from Redis set", e);
    } finally {
        jedis.close();
    }
}
}
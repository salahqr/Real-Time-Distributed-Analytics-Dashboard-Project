package Kafka_Project.Redis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

@Service
public class RedisService {
    @Autowired
    private JedisPool jedisPool;

    public void saveData(String key, String value, int time) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.setex(key, time, value);
        }
    }        

    public String getData(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.get(key);
        }
    }
    
    public void deleteData(String key) {
        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(key);
        }        
    }

    public Long incrementCounter(String key, int expireSeconds) {
        try (Jedis jedis = jedisPool.getResource()) {
            Long count = jedis.incr(key);
            if (count == 1) {
                jedis.expire(key, expireSeconds);
            }
            return count;
        }
    }
}
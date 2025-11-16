package Kafka_Project.Redis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class RateLimiter {
    
    @Autowired
    private RedisService redisService;

    public boolean rateLimiter(String key) {
        try {
            Long requestCount = redisService.incrementCounter(key, 3600);
            return requestCount <= 100;
        } catch (Exception e) {
            return false;
        }
    }
}

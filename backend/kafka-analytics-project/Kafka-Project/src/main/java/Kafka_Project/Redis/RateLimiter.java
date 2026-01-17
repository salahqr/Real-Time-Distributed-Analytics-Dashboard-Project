package Kafka_Project.Redis;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

@Component
public class RateLimiter {
    
    @Autowired
    private RedisService redisService;


    @Value("${spring.data.redis.ssl.enabled}")
    private boolean rateLimitEnabled;

    public boolean rateLimiter(String key) {
        try {
            if (!rateLimitEnabled) {
                return true; 
            }
            Long requestCount = redisService.incrementCounter(key, 3600);
            return requestCount <= 100;
        } catch (Exception e) {
            return false;
        }
    }
}

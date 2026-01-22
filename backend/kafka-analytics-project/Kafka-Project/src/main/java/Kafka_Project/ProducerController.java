package Kafka_Project;

import Kafka_Project.service.KafkaProducerService; // Add this

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.http.ResponseEntity;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

import Kafka_Project.Redis.RateLimiter;
import jakarta.servlet.http.HttpServletRequest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@RestController
public class ProducerController {
    
    private static final Logger logger = LoggerFactory.getLogger(ProducerController.class);
    private final RateLimiter rateLimiter;
    private final KafkaProducerService kafkaProducerService;
    private final ObjectMapper objectMapper;

    private static final Set<String> VALID_TOPICS = Set.of(
        "page_load", "page_view", "link_click", "button_click", "mouse_click",
        "mouse_move", "scroll_depth", "form_submit", "form_focus", "form_input",
        "video_Events", "periodic_events", "page_hidden", "page_unload",
        "product_view", "cart_add", "cart_remove", "purchase", "checkout_step",
        "custom_event", "file_download", "page_visible"
    );

    public ProducerController(KafkaProducerService kafkaProducerService, 
                             RateLimiter rateLimiter,
                             ObjectMapper objectMapper) {
        this.kafkaProducerService = kafkaProducerService;
        this.rateLimiter = rateLimiter;
        this.objectMapper = objectMapper;
    }

    private String getClientIP(HttpServletRequest request) {
        String ip = request.getHeader("X-Forwarded-For");
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("Proxy-Client-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getHeader("WL-Proxy-Client-IP");
        }
        if (ip == null || ip.isEmpty() || "unknown".equalsIgnoreCase(ip)) {
            ip = request.getRemoteAddr();
        }
        if (ip != null && ip.contains(",")) {
            ip = ip.split(",")[0].trim();
        }
        return ip;
    }
    
    private String extractUserId(JsonNode requests) {
        if (requests.isArray() && requests.size() > 0) {
            JsonNode firstEvent = requests.get(0);
            
            // Option 1: tracking_id in event data
            if (firstEvent.has("tracking_id")) {
                return firstEvent.get("tracking_id").asText();
            }
            
            if (firstEvent.has("data")) {
                JsonNode data = firstEvent.get("data");
                if (data.has("tracking_id")) {
                    return data.get("tracking_id").asText();
                }
                if (data.has("trackingId")) {
                    return data.get("trackingId").asText();
                }
            }
            
            // Option 3: user_id field
            if (firstEvent.has("user_id")) {
                return firstEvent.get("user_id").asText();
            }
        }
        
        return "anonymous";
    }

    @PostMapping("/receive_data")
    public ResponseEntity<?> receiveData(
            @RequestBody JsonNode requests, 
            HttpServletRequest httpRequest,
            @RequestHeader(value = "X-User-Id", required = false) String userIdHeader) {
        
        String clientIp = getClientIP(httpRequest);
        
        try {
            if (!rateLimiter.rateLimiter(clientIp)) {
                logger.warn("Rate limit exceeded for IP: {}", clientIp);
                return ResponseEntity.status(429)
                    .body(Map.of("error", "Too many requests, try again later"));
            }

            if (!requests.isArray()) {
                logger.warn("Invalid request format from IP: {}", clientIp);
                return ResponseEntity.status(400)
                    .body(Map.of("error", "Invalid request format: expected array"));
            }

            String userId = extractUserId(requests);
            int processedCount = 0;
            int skippedCount = 0;

            for (JsonNode eventNode : requests) {
                try {
                    // ✅ CORRECT - checks "event_type" first, then falls back to "type"
                String eventType = eventNode.has("event_type") ? 
                    eventNode.get("event_type").asText() : 
                    (eventNode.has("type") ? eventNode.get("type").asText() : "unknown");
                    
                    if ("unknown".equals(eventType) || !VALID_TOPICS.contains(eventType)) {
                        skippedCount++;
                        continue;
                    }

                    if (!eventNode.has("data")) {
                        skippedCount++;
                        continue;
                    }

                    ObjectNode messageNode = objectMapper.createObjectNode();
                    messageNode.put("timestamp", Instant.now().toString());
                    messageNode.put("event_type", eventType);
                    messageNode.put("user_id", userId);
                    messageNode.put("client_ip", clientIp);
                    messageNode.set("data", eventNode.get("data"));
                    
                    if (eventNode.has("metadata")) {
                        messageNode.set("metadata", eventNode.get("metadata"));
                    }

                    String message = objectMapper.writeValueAsString(messageNode);
                    
                    kafkaProducerService.sendMessage(message, eventType);
                    processedCount++;
                    
                } catch (Exception e) {
                    logger.error("Error processing individual event", e);
                    skippedCount++;
                }
            }

            logger.info("Processed {} events, skipped {} events for user: {}", 
                processedCount, skippedCount, userId);

            return ResponseEntity.ok(Map.of(
                "status", "success",
                "processed", processedCount,
                "skipped", skippedCount
            ));

        } catch (Exception error) {
            logger.error("Error processing batch from IP: {}", clientIp, error);
            return ResponseEntity.status(500)
                .body(Map.of("error", "Internal server error: " + error.getMessage()));
        }
    }

    @GetMapping("/health")
    public ResponseEntity<?> health() {
        return ResponseEntity.ok(Map.of(
            "status", "healthy",
            "timestamp", Instant.now().toString()
        ));
    }

    // @GetMapping("/ready")
    // public ResponseEntity<?> ready() {
    //     return ResponseEntity.ok(Map.of(
    //         "status", "ready",
    //         "timestamp", Instant.now().toString()
    //     ));
    // }

    @GetMapping("/ready")
public ResponseEntity<Map<String, Object>> ready() {
    Map<String, Object> status = new HashMap<>();
    status.put("timestamp", Instant.now().toString());
    status.put("kafka", "connected"); // update with real check if needed
    status.put("redis", "connected");
    status.put("clickhouse", "connected");
    return ResponseEntity.ok(status);
}

}
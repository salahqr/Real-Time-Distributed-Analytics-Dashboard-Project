package Kafka_Project.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Service
public class KafkaConsumerService {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
 
    public KafkaConsumerService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = new ObjectMapper();
    }

    private void insertEvent(String tableName, JsonNode event) {
        try {
            String userId = event.has("user_id") ? event.get("user_id").asText() : "anonymous";
            String eventType = event.has("event_type") ? event.get("event_type").asText() : "unknown";
            String timestamp = event.has("timestamp") ? event.get("timestamp").asText() : null;
            String clientIp = event.has("client_ip") ? event.get("client_ip").asText() : null;
            String sessionId = event.has("session_id") ? event.get("session_id").asText() : null;
            String dataJson = event.has("data") ? event.get("data").toString() : "{}";
            String metadataJson = event.has("metadata") ? event.get("metadata").toString() : "{}";
            
            String sql = "INSERT INTO analytics_events (table_name, user_id, event_type, timestamp, " +
                        "client_ip, session_id, data, metadata) " +
                        "VALUES (?, ?, ?, ?::timestamp, ?, ?, ?::jsonb, ?::jsonb)";
            
            jdbcTemplate.update(sql, tableName, userId, eventType, timestamp, 
                               clientIp, sessionId, dataJson, metadataJson);
            
            logger.debug("Inserted event: table={}, user={}, type={}", tableName, userId, eventType);
        } catch (Exception e) {
            logger.error("Failed to insert event into {}: {}", tableName, e.getMessage(), e);
            throw new RuntimeException("Database insert failed", e);
        }
    }

    @KafkaListener(
        topics = {"page_load", "page_view", "link_click", "button_click", 
                  "mouse_click", "mouse_move", "scroll_depth"}, 
        groupId = "analytics-consumers",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeUserInteractions(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            String topic = record.topic();
            String message = record.value();
            JsonNode event = objectMapper.readTree(message);
            String userId = event.has("user_id") ? event.get("user_id").asText() : "anonymous";
            
            insertEvent(topic, event);
            ack.acknowledge();
            
            logger.info("Processed {} event for user: {}", topic, userId);
        } catch (Exception e) {
            logger.error("Error processing topic: {}, offset: {}, error: {}", 
                        record.topic(), record.offset(), e.getMessage(), e);
        }
    }

    @KafkaListener(
        topics = {"form_submit", "form_focus", "form_input", "video_Events", 
                  "periodic_events", "page_hidden", "page_unload"}, 
        groupId = "analytics-consumers",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeFormAndMediaEvents(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            String topic = record.topic();
            String message = record.value();
            JsonNode event = objectMapper.readTree(message);
            String userId = event.has("user_id") ? event.get("user_id").asText() : "anonymous";
            
            insertEvent(topic, event);
            ack.acknowledge();
            
            logger.info("Processed {} event for user: {}", topic, userId);
        } catch (Exception e) {
            logger.error("Error processing topic: {}, offset: {}, error: {}", 
                        record.topic(), record.offset(), e.getMessage(), e);
        }
    }

    @KafkaListener(
        topics = {"product_view", "cart_add", "cart_remove", "purchase", 
                  "checkout_step", "custom_event", "file_download", "page_visible"}, 
        groupId = "analytics-consumers",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeEcommerceEvents(ConsumerRecord<String, String> record, Acknowledgment ack) {
        try {
            String topic = record.topic();
            String message = record.value();
            JsonNode event = objectMapper.readTree(message);
            String userId = event.has("user_id") ? event.get("user_id").asText() : "anonymous";
            
            insertEvent(topic, event);
            ack.acknowledge();
            
            logger.info("Processed {} event for user: {}", topic, userId);
        } catch (Exception e) {
            logger.error("Error processing topic: {}, offset: {}, error: {}", 
                        record.topic(), record.offset(), e.getMessage(), e);
        }
    }
}
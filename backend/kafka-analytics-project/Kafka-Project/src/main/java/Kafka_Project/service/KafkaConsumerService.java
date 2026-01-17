package Kafka_Project.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@Service
public class KafkaConsumerService {
    
    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);
    private final JdbcTemplate jdbcTemplate;
    private final ObjectMapper objectMapper;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
 
    public KafkaConsumerService(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
        this.objectMapper = new ObjectMapper();
    }

    @KafkaListener(
        topics = {"page_load", "page_view", "page_unload", "page_hidden", "page_visible"}, 
        groupId = "analytics-consumers",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumePageEvents(ConsumerRecord<String, String> record) {
        try {
            JsonNode json = objectMapper.readTree(record.value());
            
            // Get event_type from root level
            String eventType = json.path("event_type").asText();
            JsonNode data = json.path("data");
            
            // Parse timestamp
            String timestampStr = json.path("timestamp").asText();
            LocalDateTime timestamp = LocalDateTime.ofInstant(
                Instant.parse(timestampStr), 
                ZoneOffset.UTC
            );
            
            String sql = "INSERT INTO page_events (timestamp, session_id, user_id, tracking_id, " +
                        "event_type, page_url, page_title, referrer) VALUES (?, ?, ?, ?, ?, ?, ?, ?)";
            
            jdbcTemplate.update(sql,
                timestamp.format(FORMATTER),
                data.path("session_id").asText(""),
                data.path("user_id").asText(""),
                data.path("tracking_id").asText(""),
                eventType,
                data.path("url").asText(""),
                data.path("title").asText(""),
                data.path("referrer").asText("")
            );
            
            logger.info("✓ Inserted page event: {} for tracking_id: {}", eventType, data.path("tracking_id").asText());
        } catch (Exception e) {
            logger.error("✗ Error processing page event from topic {}: {}", record.topic(), e.getMessage(), e);
        }
    }

    @KafkaListener(
        topics = {"mouse_click", "button_click", "link_click", "file_download"}, 
        groupId = "analytics-consumers",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeInteractionEvents(ConsumerRecord<String, String> record) {
        try {
            JsonNode json = objectMapper.readTree(record.value());
            String eventType = json.path("event_type").asText();
            JsonNode data = json.path("data");
            
            String timestampStr = json.path("timestamp").asText();
            LocalDateTime timestamp = LocalDateTime.ofInstant(
                Instant.parse(timestampStr), 
                ZoneOffset.UTC
            );
            
            String sql = "INSERT INTO interaction_events (timestamp, session_id, user_id, tracking_id, " +
                        "event_type, page_url, element) VALUES (?, ?, ?, ?, ?, ?, ?)";
            
            jdbcTemplate.update(sql,
                timestamp.format(FORMATTER),
                data.path("session_id").asText(""),
                data.path("user_id").asText(""),
                data.path("tracking_id").asText(""),
                eventType,
                data.path("url").asText(""),
                data.path("element").asText("")
            );
            
            logger.info("✓ Inserted interaction event: {} for tracking_id: {}", eventType, data.path("tracking_id").asText());
        } catch (Exception e) {
            logger.error("✗ Error processing interaction event from topic {}: {}", record.topic(), e.getMessage(), e);
        }
    }

    @KafkaListener(
        topics = {"form_submit", "form_focus", "form_input"}, 
        groupId = "analytics-consumers",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consumeFormEvents(ConsumerRecord<String, String> record) {
        try {
            JsonNode json = objectMapper.readTree(record.value());
            String eventType = json.path("event_type").asText();
            JsonNode data = json.path("data");
            
            String timestampStr = json.path("timestamp").asText();
            LocalDateTime timestamp = LocalDateTime.ofInstant(
                Instant.parse(timestampStr), 
                ZoneOffset.UTC
            );
            
            String sql = "INSERT INTO form_events (timestamp, session_id, user_id, tracking_id, " +
                        "page_url, event_type, form_id) VALUES (?, ?, ?, ?, ?, ?, ?)";
            
            jdbcTemplate.update(sql,
                timestamp.format(FORMATTER),
                data.path("session_id").asText(""),
                data.path("user_id").asText(""),
                data.path("tracking_id").asText(""),
                data.path("url").asText(""),
                eventType,
                data.path("form_id").asText("")
            );
            
            logger.info("✓ Inserted form event: {} for tracking_id: {}", eventType, data.path("tracking_id").asText());
        } catch (Exception e) {
            logger.error("✗ Error processing form event from topic {}: {}", record.topic(), e.getMessage(), e);
        }
    }
    
    
    @KafkaListener(
    topics = {"product_view", "cart_add", "cart_remove", "checkout_step", "purchase"},
    groupId = "analytics-consumers",
    containerFactory = "kafkaListenerContainerFactory"
)
public void consumeEcommerceEvents(ConsumerRecord<String, String> record) {

    logger.warn("🔥 CONSUMER TRIGGERED | topic={} offset={}",
            record.topic(), record.offset());

    logger.warn("📦 RAW MESSAGE: {}", record.value());

    try {
        JsonNode json = objectMapper.readTree(record.value());
        JsonNode data = json.path("data");

        String eventType = json.has("event_type") ? json.get("event_type").asText() : null;
        String trackingId = data.path("tracking_id").asText(null);

        logger.warn("🔍 eventType={} trackingId={}", eventType, trackingId);

        if (eventType == null || trackingId == null) {
            logger.error("❌ INVALID MESSAGE — missing required fields");
            return;
        }

        // ✅ Handle timestamp correctly
        String tsStr = json.has("timestamp") ? json.get("timestamp").asText()
                        : data.has("timestamp") ? data.get("timestamp").asText()
                        : null;

        LocalDateTime timestamp;
        if (tsStr != null) {
            try {
                timestamp = LocalDateTime.ofInstant(Instant.parse(tsStr), ZoneOffset.UTC);
            } catch (Exception e) {
                timestamp = LocalDateTime.now();
                logger.warn("⚠ Timestamp parse failed, fallback to now()");
            }
        } else {
            timestamp = LocalDateTime.now();
            logger.warn("⚠ Timestamp missing, fallback to now()");
        }

        // 🔧 ADD THIS - Define the formatter
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String timestampStr = timestamp.format(formatter);

        logger.warn("⏱ timestamp={}", timestampStr);

        // ✅ Insert into ClickHouse with proper spacing and String timestamp
        jdbcTemplate.update(
            "INSERT INTO default.ecommerce_events " +
            "(timestamp, session_id, user_id, tracking_id, page_url, event_type, " +
            "product_id, product_name, price, quantity, category, currency, " +
            "order_id, total, step, step_name) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            timestampStr,  // 🔧 Changed from timestamp to timestampStr
            data.path("session_id").asText(null),
            data.path("user_id").asText(null),
            trackingId,
            data.path("page_url").asText(null),
            eventType,
            data.path("product_id").isMissingNode() ? null : data.path("product_id").asText(),
            data.path("product_name").isMissingNode() ? null : data.path("product_name").asText(),
            data.path("price").isMissingNode() ? null : data.path("price").asDouble(),
            data.path("quantity").isMissingNode() ? null : data.path("quantity").asInt(),
            data.path("category").isMissingNode() ? null : data.path("category").asText(),
            data.path("currency").isMissingNode() ? "USD" : data.path("currency").asText(),
            data.path("order_id").isMissingNode() ? null : data.path("order_id").asText(),
            data.path("total").isMissingNode() ? null : data.path("total").asDouble(),
            data.path("step").isMissingNode() ? null : data.path("step").asInt(),
            data.path("step_name").isMissingNode() ? null : data.path("step_name").asText()
        );

        logger.warn("✅ INSERT SUCCESS");

    } catch (Exception e) {
        logger.error("💥 CONSUMER FAILED | topic={} | payload={}",
                record.topic(), record.value(), e);
    }
}
}


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

    logger.info(
            "🟢 KAFKA CONSUMER START | topic={} partition={} offset={} key={}",
            record.topic(),
            record.partition(),
            record.offset(),
            record.key()
    );

    logger.info("📦 RAW MESSAGE: {}", record.value());

    try {
        // ===============================
        // 1️⃣ Parse JSON
        // ===============================
        JsonNode json = objectMapper.readTree(record.value());
        JsonNode data = json.path("data");

        if (!json.has("event_type") || data.isMissingNode()) {
            logger.error("❌ INVALID JSON STRUCTURE | json={}", json);
            return;
        }

        String eventType = json.get("event_type").asText();
        String trackingId = data.path("tracking_id").asText(null);

        if (trackingId == null) {
            logger.error("❌ MISSING tracking_id | data={}", data);
            return;
        }

        logger.info("🔍 eventType={} trackingId={}", eventType, trackingId);

        // ===============================
        // 2️⃣ Timestamp handling
        // ===============================
        String tsStr = json.has("timestamp")
                ? json.get("timestamp").asText()
                : data.path("timestamp").asText(null);

        LocalDateTime timestamp;
        try {
            timestamp = tsStr != null
                    ? LocalDateTime.ofInstant(Instant.parse(tsStr), ZoneOffset.UTC)
                    : LocalDateTime.now();
        } catch (Exception e) {
            logger.warn("⚠ Timestamp parse failed, fallback to now()");
            timestamp = LocalDateTime.now();
        }

        timestamp = timestamp.withNano(0);
        logger.info("⏱ timestamp={}", timestamp);

        // ===============================
        // 3️⃣ SAFE TYPE CONVERSIONS
        // ===============================
        Float price = data.has("price") && !data.get("price").isNull()
                ? (float) data.get("price").asDouble()
                : null;

        Float total = data.has("total") && !data.get("total").isNull()
                ? (float) data.get("total").asDouble()
                : null;

        Integer quantity = data.has("quantity") && !data.get("quantity").isNull()
                ? Math.max(0, data.get("quantity").asInt())
                : null;

        Integer step = data.has("step") && !data.get("step").isNull()
                ? Math.min(255, Math.max(0, data.get("step").asInt()))
                : null;

        logger.info(
                "🧪 INSERT VALUES | price={} quantity={} total={} step={}",
                price, quantity, total, step
        );

        // ===============================
        // 4️⃣ INSERT INTO CLICKHOUSE
        // ===============================
        jdbcTemplate.update(
                "INSERT INTO ecommerce_events (" +
                        "timestamp, session_id, user_id, tracking_id, page_url, event_type, " +
                        "product_id, product_name, price, quantity, category, currency, " +
                        "order_id, total, step, step_name" +
                        ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                timestamp,
                data.path("session_id").asText(null),
                data.path("user_id").asText(null),
                trackingId,
                data.path("page_url").asText(null),
                eventType,
                data.path("product_id").isMissingNode() ? null : data.path("product_id").asText(),
                data.path("product_name").isMissingNode() ? null : data.path("product_name").asText(),
                price,
                quantity,
                data.path("category").isMissingNode() ? null : data.path("category").asText(),
                data.path("currency").isMissingNode() ? "USD" : data.path("currency").asText(),
                data.path("order_id").isMissingNode() ? null : data.path("order_id").asText(),
                total,
                step,
                data.path("step_name").isMissingNode() ? null : data.path("step_name").asText()
        );

        logger.info("✅ INSERT SUCCESS | trackingId={} offset={}",
                trackingId, record.offset());

    } catch (Exception e) {
        logger.error(
                "💥 CONSUMER FAILED | topic={} offset={} payload={}",
                record.topic(),
                record.offset(),
                record.value(),
                e
        );
    }
}

}
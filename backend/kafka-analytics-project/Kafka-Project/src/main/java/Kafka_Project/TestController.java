package Kafka_Project;

import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.http.ResponseEntity;
import org.springframework.http.HttpStatus;
import org.springframework.jdbc.core.JdbcTemplate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Map;
import java.util.HashMap;

@RestController
@RequestMapping("/test")
public class TestController {

    private final JdbcTemplate jdbcTemplate;
    private final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public TestController(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    private Map<String, Object> createEvent(String sessionId, String userId, String trackingId, 
            String pageUrl, String productId, String productName, double price, int quantity,
            String category, String currency, String orderId, double total, int step, String stepName) {
        Map<String, Object> event = new HashMap<>();
        event.put("session_id", sessionId);
        event.put("user_id", userId);
        event.put("tracking_id", trackingId);
        event.put("page_url", pageUrl);
        event.put("product_id", productId);
        event.put("product_name", productName);
        event.put("price", price);
        event.put("quantity", quantity);
        event.put("category", category);
        event.put("currency", currency);
        event.put("order_id", orderId);
        event.put("total", total);
        event.put("step", step);
        event.put("step_name", stepName);
        return event;
    }

    @PostMapping("/val")
    public ResponseEntity<Map<String, Object>> insertTestPurchases() {
        int inserted = 0;

        try {
            List<Map<String, Object>> events = List.of(
                createEvent("sess-test-1", "user-1", "track-1", "https://example.com/product/123",
                    "P1", "Wireless Headphones", 99.99, 1, "Electronics", "USD",
                    "ORD-1", 99.99, 4, "payment"),
                createEvent("sess-test-2", "user-2", "track-2", "https://example.com/product/456",
                    "P2", "Smartwatch", 199.99, 2, "Electronics", "USD",
                    "ORD-2", 399.98, 4, "payment"),
                createEvent("sess-test-3", "user-3", "track-3", "https://example.com/product/789",
                    "P3", "Gaming Mouse", 49.99, 1, "Electronics", "USD",
                    "ORD-3", 49.99, 4, "payment")
            );

            for (Map<String, Object> event : events) {
                // تحويل LocalDateTime إلى String
                String timestampStr = LocalDateTime.now().format(formatter);

                jdbcTemplate.update(
                    "INSERT INTO default.ecommerce_events " +
                    "(timestamp, session_id, user_id, tracking_id, page_url, event_type, " +
                    "product_id, product_name, price, quantity, category, currency, " +
                    "order_id, total, step, step_name) " +
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    timestampStr,
                    event.get("session_id"),
                    event.get("user_id"),
                    event.get("tracking_id"),
                    event.get("page_url"),
                    "purchase", // event_type
                    event.get("product_id"),
                    event.get("product_name"),
                    event.get("price"),
                    event.get("quantity"),
                    event.get("category"),
                    event.get("currency"),
                    event.get("order_id"),
                    event.get("total"),
                    event.get("step"),
                    event.get("step_name")
                );
                inserted++;
            }

            Map<String, Object> response = new HashMap<>();
            response.put("status", "success");
            response.put("inserted", inserted);
            return ResponseEntity.ok(response);

        } catch (Exception e) {
            Map<String, Object> response = new HashMap<>();
            response.put("status", "error");
            response.put("message", e.getMessage());
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(response);
        }
    }
}

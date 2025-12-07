package Kafka_Project;

import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ActiveProfiles;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "spring.kafka.bootstrap-servers=localhost:9092",
        "spring.data.redis.host=localhost",
        "spring.data.redis.port=6379",
        "spring.datasource.url=jdbc:clickhouse://localhost:8123/default?use_binary_format=false",
        "spring.datasource.username=default",
        "spring.datasource.password=root"
    }
)
@ActiveProfiles("test")
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealIntegrationTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String TEST_TRACKING_ID = "test-tracking-real-" + System.currentTimeMillis();
    private static final String TEST_SESSION_ID = "test-session-real-" + System.currentTimeMillis();
    private static final String TEST_USER_ID = "test-user-real-" + System.currentTimeMillis();

    @BeforeAll
    void setupDatabase() {
        System.out.println("=================================================");
        System.out.println("Starting Real Integration Tests");
        System.out.println("Tracking ID: " + TEST_TRACKING_ID);
        System.out.println("Session ID: " + TEST_SESSION_ID);
        System.out.println("User ID: " + TEST_USER_ID);
        System.out.println("=================================================");
        System.out.println("\n⚠️  IMPORTANT: If you get 429 errors, run: redis-cli FLUSHALL\n");

        // Create test user
        try {
            String createUser = 
                "INSERT INTO users (user_id, company_name, email, password, is_verify, created_at) " +
                "VALUES (?, 'Real Test Company', 'realtest@example.com', 'password123', 1, now())";
            jdbcTemplate.update(createUser, TEST_USER_ID);
            System.out.println("✓ Test user created");
        } catch (Exception e) {
            System.out.println("⚠ User might already exist: " + e.getMessage());
        }
    }

    @BeforeEach
    void waitBetweenTests() throws InterruptedException {
        // Wait 3 seconds between tests to avoid rate limiting
        System.out.println("⏳ Waiting 3 seconds to avoid rate limiting...");
        TimeUnit.SECONDS.sleep(3);
    }

    @AfterAll
    void cleanup() {
        System.out.println("\n=================================================");
        System.out.println("Cleaning up test data...");
        System.out.println("=================================================");

        // Clean up test data
        try {
            jdbcTemplate.update("DELETE FROM users WHERE user_id = ?", TEST_USER_ID);
            jdbcTemplate.update("ALTER TABLE sessions DELETE WHERE tracking_id = ?", TEST_TRACKING_ID);
            jdbcTemplate.update("ALTER TABLE page_events DELETE WHERE tracking_id = ?", TEST_TRACKING_ID);
            jdbcTemplate.update("ALTER TABLE interaction_events DELETE WHERE tracking_id = ?", TEST_TRACKING_ID);
            jdbcTemplate.update("ALTER TABLE mouse_events DELETE WHERE tracking_id = ?", TEST_TRACKING_ID);
            jdbcTemplate.update("ALTER TABLE scroll_events DELETE WHERE tracking_id = ?", TEST_TRACKING_ID);
            jdbcTemplate.update("ALTER TABLE form_events DELETE WHERE tracking_id = ?", TEST_TRACKING_ID);
            jdbcTemplate.update("ALTER TABLE video_events DELETE WHERE tracking_id = ?", TEST_TRACKING_ID);
            jdbcTemplate.update("ALTER TABLE ecommerce_events DELETE WHERE tracking_id = ?", TEST_TRACKING_ID);
            jdbcTemplate.update("ALTER TABLE custom_events DELETE WHERE tracking_id = ?", TEST_TRACKING_ID);
            System.out.println("✓ Test data cleaned up");
        } catch (Exception e) {
            System.out.println("⚠ Cleanup warning: " + e.getMessage());
        }
    }

    private void waitForDataProcessing() throws InterruptedException {
        System.out.println("⏳ Waiting for Kafka consumer to process data (5 seconds)...");
        TimeUnit.SECONDS.sleep(5);
    }

    @Test
    @Order(1)
    void testHealthEndpoint() {
        System.out.println("\n--- Test 1: Health Check ---");
        ResponseEntity<String> response = restTemplate.getForEntity("/health", String.class);
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        System.out.println("✓ Health check passed: " + response.getBody());
    }

    @Test
    @Order(2)
    void testPageLoadEventToDatabase() throws InterruptedException {
        System.out.println("\n--- Test 2: Page Load Event ---");
        
        String pageLoadData = String.format("""
        [
            {
                "type": "page_load",
                "data": {
                    "session_id": "%s",
                    "user_id": "%s",
                    "tracking_id": "%s",
                    "url": "https://example.com/home",
                    "referrer": "https://google.com",
                    "title": "Home Page - Real Test",
                    "screen_resolution": {
                        "width": 1920,
                        "height": 1080
                    },
                    "viewport": {
                        "width": 1440,
                        "height": 900
                    },
                    "operating_system": "MacIntel",
                    "browser": "Mozilla/5.0 Chrome/120.0",
                    "language": "en-US",
                    "timezone": "America/New_York",
                    "device_type": "Desktop",
                    "location": {
                        "country": "United States",
                        "country_code": "US"
                    },
                    "performance": {
                        "dns_time": 50,
                        "connect_time": 100,
                        "response_time": 200,
                        "dom_load_time": 500,
                        "page_load_time": 1200
                    }
                }
            }
        ]
        """, TEST_SESSION_ID, TEST_USER_ID, TEST_TRACKING_ID);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(pageLoadData, headers);

        ResponseEntity<String> response = restTemplate.postForEntity("/receive_data", request, String.class);
        
        System.out.println("Response status: " + response.getStatusCode());
        System.out.println("Response body: " + response.getBody());
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertTrue(response.getBody().contains("success") || response.getBody().contains("processed"));
        System.out.println("✓ Page load event sent to Kafka");

        waitForDataProcessing();

        // Verify data in ClickHouse
        String query = "SELECT COUNT(*) FROM page_events WHERE tracking_id = ? AND event_type = 'page_load'";
        Integer count = jdbcTemplate.queryForObject(query, Integer.class, TEST_TRACKING_ID);
        assertTrue(count > 0, "Page load event should be in database");
        System.out.println("✓ Page load event found in database: " + count + " records");
    }

    @Test
    @Order(3)
    void testClickEventsToDatabase() throws InterruptedException {
        System.out.println("\n--- Test 3: Click Events ---");
        
        String clickData = String.format("""
        [
            {
                "type": "mouse_click",
                "data": {
                    "session_id": "%s",
                    "user_id": "%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/products",
                    "x": 450,
                    "y": 300,
                    "element": "button",
                    "element_id": "buy-now-btn",
                    "element_class": "btn btn-primary"
                }
            },
            {
                "type": "button_click",
                "data": {
                    "session_id": "%s",
                    "user_id": "%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/products",
                    "button_text": "Add to Cart",
                    "button_type": "submit",
                    "button_id": "add-cart-btn"
                }
            },
            {
                "type": "link_click",
                "data": {
                    "session_id": "%s",
                    "user_id": "%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/products",
                    "link_url": "https://example.com/product/123",
                    "link_text": "View Product Details",
                    "is_external": false,
                    "target": "_self"
                }
            }
        ]
        """, TEST_SESSION_ID, TEST_USER_ID, TEST_TRACKING_ID,
             TEST_SESSION_ID, TEST_USER_ID, TEST_TRACKING_ID,
             TEST_SESSION_ID, TEST_USER_ID, TEST_TRACKING_ID);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(clickData, headers);

        ResponseEntity<String> response = restTemplate.postForEntity("/receive_data", request, String.class);
        
        System.out.println("Response: " + response.getBody());
        assertEquals(HttpStatus.OK, response.getStatusCode());
        System.out.println("✓ Click events sent to Kafka");

        waitForDataProcessing();

        // Verify in database
        String query = "SELECT COUNT(*) FROM interaction_events WHERE tracking_id = ?";
        Integer count = jdbcTemplate.queryForObject(query, Integer.class, TEST_TRACKING_ID);
        assertTrue(count >= 3, "Should have at least 3 click events in database, got: " + count);
        System.out.println("✓ Click events found in database: " + count + " records");
    }

    @Test
    @Order(4)
    void testFormEventsToDatabase() throws InterruptedException {
        System.out.println("\n--- Test 4: Form Events ---");
        
        String formData = String.format("""
        [
            {
                "type": "form_focus",
                "data": {
                    "session_id": "%s",
                    "user_id": "%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/contact",
                    "form_id": "contact-form-real",
                    "form_name": "contact",
                    "field_name": "email",
                    "field_type": "email"
                }
            },
            {
                "type": "form_submit",
                "data": {
                    "session_id": "%s",
                    "user_id": "%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/contact",
                    "form_id": "contact-form-real",
                    "form_name": "contact",
                    "form_action": "/api/contact",
                    "form_method": "POST",
                    "field_count": 5,
                    "has_file_upload": false,
                    "success": true
                }
            }
        ]
        """, TEST_SESSION_ID, TEST_USER_ID, TEST_TRACKING_ID,
             TEST_SESSION_ID, TEST_USER_ID, TEST_TRACKING_ID);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(formData, headers);

        ResponseEntity<String> response = restTemplate.postForEntity("/receive_data", request, String.class);
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        System.out.println("✓ Form events sent to Kafka");

        waitForDataProcessing();

        // Verify in database
        String query = "SELECT COUNT(*) FROM form_events WHERE tracking_id = ? AND form_id = 'contact-form-real'";
        Integer count = jdbcTemplate.queryForObject(query, Integer.class, TEST_TRACKING_ID);
        assertTrue(count >= 2, "Should have at least 2 form events in database, got: " + count);
        System.out.println("✓ Form events found in database: " + count + " records");
    }

    @Test
    @Order(5)
    void testEcommerceEventsToDatabase() throws InterruptedException {
        System.out.println("\n--- Test 5: E-commerce Events ---");
        
        String orderId = "ORDER-REAL-" + System.currentTimeMillis();
        String ecommerceData = String.format("""
        [
            {
                "type": "product_view",
                "data": {
                    "session_id": "%s",
                    "user_id": "%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/product/999",
                    "product_id": "PROD-REAL-999",
                    "product_name": "Real Test Product",
                    "price": 199.99,
                    "category": "Test Category",
                    "currency": "USD"
                }
            },
            {
                "type": "cart_add",
                "data": {
                    "session_id": "%s",
                    "user_id": "%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/product/999",
                    "product_id": "PROD-REAL-999",
                    "product_name": "Real Test Product",
                    "price": 199.99,
                    "quantity": 2,
                    "category": "Test Category",
                    "currency": "USD"
                }
            },
            {
                "type": "purchase",
                "data": {
                    "session_id": "%s",
                    "user_id": "%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/order-confirmation",
                    "order_id": "%s",
                    "product_id": "PROD-REAL-999",
                    "product_name": "Real Test Product",
                    "price": 199.99,
                    "quantity": 2,
                    "total": 419.98,
                    "currency": "USD"
                }
            }
        ]
        """, TEST_SESSION_ID, TEST_USER_ID, TEST_TRACKING_ID,
             TEST_SESSION_ID, TEST_USER_ID, TEST_TRACKING_ID,
             TEST_SESSION_ID, TEST_USER_ID, TEST_TRACKING_ID, orderId);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(ecommerceData, headers);

        ResponseEntity<String> response = restTemplate.postForEntity("/receive_data", request, String.class);
        
        assertEquals(HttpStatus.OK, response.getStatusCode());
        System.out.println("✓ E-commerce events sent to Kafka");

        waitForDataProcessing();

        // Verify in database
        String query = "SELECT COUNT(*) FROM ecommerce_events WHERE tracking_id = ? AND product_id = 'PROD-REAL-999'";
        Integer count = jdbcTemplate.queryForObject(query, Integer.class, TEST_TRACKING_ID);
        assertTrue(count >= 3, "Should have at least 3 e-commerce events in database, got: " + count);
        System.out.println("✓ E-commerce events found in database: " + count + " records");

        // Verify purchase specifically
        String purchaseQuery = "SELECT COUNT(*) FROM ecommerce_events WHERE tracking_id = ? AND event_type = 'purchase'";
        Integer purchaseCount = jdbcTemplate.queryForObject(purchaseQuery, Integer.class, TEST_TRACKING_ID);
        assertTrue(purchaseCount >= 1, "Should have at least 1 purchase event, got: " + purchaseCount);
        System.out.println("✓ Purchase event verified: " + purchaseCount + " records");
    }

    @Test
    @Order(6)
    void testDataIntegrityAcrossTables() throws InterruptedException {
        System.out.println("\n--- Test 6: Data Integrity Check ---");
        
        // Wait a bit more to ensure all data is processed
        TimeUnit.SECONDS.sleep(3);

        // Check all tables for our test data
        Map<String, String> tables = Map.of(
            "page_events", "SELECT COUNT(*) FROM page_events WHERE tracking_id = ?",
            "interaction_events", "SELECT COUNT(*) FROM interaction_events WHERE tracking_id = ?",
            "form_events", "SELECT COUNT(*) FROM form_events WHERE tracking_id = ?",
            "ecommerce_events", "SELECT COUNT(*) FROM ecommerce_events WHERE tracking_id = ?"
        );

        System.out.println("\nData Distribution Across Tables:");
        System.out.println("─────────────────────────────────────");
        
        int totalRecords = 0;
        for (Map.Entry<String, String> entry : tables.entrySet()) {
            try {
                Integer count = jdbcTemplate.queryForObject(entry.getValue(), Integer.class, TEST_TRACKING_ID);
                System.out.printf("%-25s: %d records%n", entry.getKey(), count);
                totalRecords += count;
            } catch (Exception e) {
                System.out.printf("%-25s: ERROR - %s%n", entry.getKey(), e.getMessage());
            }
        }
        
        System.out.println("─────────────────────────────────────");
        System.out.printf("%-25s: %d records%n", "TOTAL", totalRecords);
        System.out.println("─────────────────────────────────────");

        assertTrue(totalRecords > 0, "Should have data in at least one table");
        System.out.println("✓ Data integrity check passed");
    }
}
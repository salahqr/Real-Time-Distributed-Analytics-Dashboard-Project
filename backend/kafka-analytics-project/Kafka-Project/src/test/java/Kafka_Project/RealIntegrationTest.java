package Kafka_Project;

import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Integration tests that verify the entire Docker cluster is working.
 * Requires: docker compose up -d
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "spring.datasource.url=jdbc:clickhouse://localhost:8123/default?use_binary_format=false",
        "spring.datasource.username=default",
<<<<<<< HEAD
        "spring.datasource.password=root"  // ✅ correct password
=======
        "spring.datasource.password=root"
>>>>>>> 39bef9c52dfbe4ac90b6246fa6fb564fb40b1660
    }
)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class RealIntegrationTest {

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private final RestTemplate restTemplate = new RestTemplate();
    private static final String DOCKER_CLUSTER_URL = "http://localhost:8080"; // Nginx load balancer
    private static final String TEST_TRACKING_ID = "integration-test-" + System.currentTimeMillis();
<<<<<<< HEAD
    private static final int KAFKA_WAIT_SECONDS = 15; // Increased wait time~

=======
>>>>>>> 39bef9c52dfbe4ac90b6246fa6fb564fb40b1660

    @BeforeAll
    static void checkDockerRunning() {
        System.out.println("=================================================");
        System.out.println("REAL INTEGRATION TEST - Testing Docker Cluster");
        System.out.println("=================================================");
        System.out.println("⚠️  PREREQUISITE: Run 'docker compose up -d' first!");
        System.out.println("=================================================\n");
    }

    @Test
    @Order(1)
    void testHealthEndpoint() {
        System.out.println("\n--- Test 1: Cluster Health Check ---");
        
        try {
            ResponseEntity<String> response = restTemplate.getForEntity(
                DOCKER_CLUSTER_URL + "/health", 
                String.class
            );
            
            assertEquals(HttpStatus.OK, response.getStatusCode());
            System.out.println("✓ Docker cluster is responding");
        } catch (Exception e) {
            fail("Docker cluster not running! Run: docker compose up -d");
        }
    }

    @Test
    @Order(2)
    void testPageLoadEventToDatabase() throws InterruptedException {
        System.out.println("\n--- Test 2: Page Load Event End-to-End ---");
        
        String eventData = String.format("""
        [
            {
                "type": "page_load",
                "data": {
                    "session_id": "session-%s",
                    "user_id": "user-%s",
                    "tracking_id": "%s",
                    "url": "https://example.com/test",
                    "referrer": "https://google.com",
                    "title": "Integration Test Page",
                    "screen_resolution": {"width": 1920, "height": 1080},
                    "viewport": {"width": 1440, "height": 900},
                    "operating_system": "Linux",
                    "browser": "Chrome",
                    "language": "en-US",
                    "timezone": "UTC",
                    "device_type": "Desktop",
                    "location": {"country": "Test", "country_code": "TS"},
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
        """, TEST_TRACKING_ID, TEST_TRACKING_ID, TEST_TRACKING_ID);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(eventData, headers);

        ResponseEntity<String> response = restTemplate.postForEntity(
            DOCKER_CLUSTER_URL + "/receive_data",
            request,
            String.class
        );

        System.out.println("API Response: " + response.getBody());
        assertEquals(HttpStatus.OK, response.getStatusCode());

        // Wait for Kafka consumers to process
        System.out.println("⏳ Waiting 10 seconds for Kafka consumers...");
        TimeUnit.SECONDS.sleep(10);

        // Verify in ClickHouse
        String query = "SELECT COUNT(*) FROM page_events WHERE tracking_id = ?";
        Integer count = jdbcTemplate.queryForObject(query, Integer.class, TEST_TRACKING_ID);
        
        System.out.println("✓ Found " + count + " page load events in ClickHouse");
        assertTrue(count > 0, "Page load event should be in database");
    }

    @Test
    @Order(3)
    void testClickEventsToDatabase() throws InterruptedException {
        System.out.println("\n--- Test 3: Click Events End-to-End ---");
        
        String clickData = String.format("""
        [
            {
                "type": "mouse_click",
                "data": {
                    "session_id": "session-%s",
                    "user_id": "user-%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/clicks",
                    "x": 450,
                    "y": 300,
                    "element": "button",
                    "element_id": "test-button"
                }
            },
            {
                "type": "button_click",
                "data": {
                    "session_id": "session-%s",
                    "user_id": "user-%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/clicks",
                    "button_text": "Click Me",
                    "button_type": "submit",
                    "button_id": "submit-btn"
                }
            },
            {
                "type": "link_click",
                "data": {
                    "session_id": "session-%s",
                    "user_id": "user-%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/clicks",
                    "link_url": "https://example.com/target",
                    "link_text": "Test Link",
                    "is_external": false
                }
            }
        ]
        """, TEST_TRACKING_ID, TEST_TRACKING_ID, TEST_TRACKING_ID,
             TEST_TRACKING_ID, TEST_TRACKING_ID, TEST_TRACKING_ID,
             TEST_TRACKING_ID, TEST_TRACKING_ID, TEST_TRACKING_ID);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(clickData, headers);

        restTemplate.postForEntity(DOCKER_CLUSTER_URL + "/receive_data", request, String.class);

        TimeUnit.SECONDS.sleep(10);

        String query = "SELECT COUNT(*) FROM interaction_events WHERE tracking_id = ?";
        Integer count = jdbcTemplate.queryForObject(query, Integer.class, TEST_TRACKING_ID);
        
        System.out.println("✓ Found " + count + " click events in ClickHouse");
        assertTrue(count >= 3, "Should have at least 3 click events, got: " + count);
    }

    @Test
    @Order(4)
    void testFormEventsToDatabase() throws InterruptedException {
        System.out.println("\n--- Test 4: Form Events End-to-End ---");
        
        String formData = String.format("""
        [
            {
                "type": "form_focus",
                "data": {
                    "session_id": "session-%s",
                    "user_id": "user-%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/form",
                    "form_id": "test-form",
                    "field_name": "email",
                    "field_type": "email"
                }
            },
            {
                "type": "form_submit",
                "data": {
                    "session_id": "session-%s",
                    "user_id": "user-%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/form",
                    "form_id": "test-form",
                    "form_name": "contact",
                    "form_action": "/submit",
                    "form_method": "POST",
                    "field_count": 3,
                    "success": true
                }
            }
        ]
        """, TEST_TRACKING_ID, TEST_TRACKING_ID, TEST_TRACKING_ID,
             TEST_TRACKING_ID, TEST_TRACKING_ID, TEST_TRACKING_ID);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(formData, headers);

        restTemplate.postForEntity(DOCKER_CLUSTER_URL + "/receive_data", request, String.class);

        TimeUnit.SECONDS.sleep(10);

        String query = "SELECT COUNT(*) FROM form_events WHERE tracking_id = ?";
        Integer count = jdbcTemplate.queryForObject(query, Integer.class, TEST_TRACKING_ID);
        
        System.out.println("✓ Found " + count + " form events in ClickHouse");
        assertTrue(count >= 2, "Should have at least 2 form events, got: " + count);
    }

    @Test
    @Order(5)
    void testEcommerceEventsToDatabase() throws InterruptedException {
        System.out.println("\n--- Test 5: E-commerce Events End-to-End ---");
        
        String ecommerceData = String.format("""
        [
            {
                "type": "product_view",
                "data": {
                    "session_id": "session-%s",
                    "user_id": "user-%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/product",
                    "product_id": "TEST-PROD",
                    "product_name": "Test Product",
                    "price": 99.99,
                    "category": "Test"
                }
            },
            {
                "type": "cart_add",
                "data": {
                    "session_id": "session-%s",
                    "user_id": "user-%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/cart",
                    "product_id": "TEST-PROD",
                    "product_name": "Test Product",
                    "price": 99.99,
                    "quantity": 1
                }
            },
            {
                "type": "purchase",
                "data": {
                    "session_id": "session-%s",
                    "user_id": "user-%s",
                    "tracking_id": "%s",
                    "page_url": "https://example.com/checkout",
                    "order_id": "TEST-ORDER",
                    "product_id": "TEST-PROD",
                    "product_name": "Test Product",
                    "price": 99.99,
                    "quantity": 1,
                    "total": 99.99
                }
            }
        ]
        """, TEST_TRACKING_ID, TEST_TRACKING_ID, TEST_TRACKING_ID,
             TEST_TRACKING_ID, TEST_TRACKING_ID, TEST_TRACKING_ID,
             TEST_TRACKING_ID, TEST_TRACKING_ID, TEST_TRACKING_ID);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        HttpEntity<String> request = new HttpEntity<>(ecommerceData, headers);

        restTemplate.postForEntity(DOCKER_CLUSTER_URL + "/receive_data", request, String.class);

        TimeUnit.SECONDS.sleep(10);

        String query = "SELECT COUNT(*) FROM ecommerce_events WHERE tracking_id = ?";
        Integer count = jdbcTemplate.queryForObject(query, Integer.class, TEST_TRACKING_ID);
        
        System.out.println("✓ Found " + count + " e-commerce events in ClickHouse");
        assertTrue(count >= 3, "Should have at least 3 e-commerce events, got: " + count);
    }

    @Test
    @Order(6)
    void testDataIntegrityAcrossTables() {
        System.out.println("\n--- Test 6: Data Integrity Across All Tables ---");
        
        int totalCount = 0;
        
        totalCount += jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM page_events WHERE tracking_id = ?", 
            Integer.class, TEST_TRACKING_ID
        );
        totalCount += jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM interaction_events WHERE tracking_id = ?", 
            Integer.class, TEST_TRACKING_ID
        );
        totalCount += jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM form_events WHERE tracking_id = ?", 
            Integer.class, TEST_TRACKING_ID
        );
        totalCount += jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM ecommerce_events WHERE tracking_id = ?", 
            Integer.class, TEST_TRACKING_ID
        );
        
        System.out.println("✓ Total events across all tables: " + totalCount);
        assertTrue(totalCount > 0, "Should have data in at least one table");
    }

    @AfterAll
    static void cleanup(@Autowired JdbcTemplate jdbcTemplate) {
        System.out.println("\n=================================================");
        System.out.println("Cleaning up test data...");
        System.out.println("=================================================\n");
        
        try {
            String trackingId = "integration-test-%";
            jdbcTemplate.update("ALTER TABLE page_events DELETE WHERE tracking_id LIKE ?", trackingId);
            jdbcTemplate.update("ALTER TABLE interaction_events DELETE WHERE tracking_id LIKE ?", trackingId);
            jdbcTemplate.update("ALTER TABLE form_events DELETE WHERE tracking_id LIKE ?", trackingId);
            jdbcTemplate.update("ALTER TABLE ecommerce_events DELETE WHERE tracking_id LIKE ?", trackingId);
            System.out.println("✓ Test data cleaned up");
        } catch (Exception e) {
            System.out.println("⚠ Cleanup warning: " + e.getMessage());
        }
    }
}

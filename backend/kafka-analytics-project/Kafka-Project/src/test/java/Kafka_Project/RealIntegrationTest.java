package Kafka_Project;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.MvcResult;

import Kafka_Project.Redis.RateLimiter;
import Kafka_Project.Redis.RedisService;

import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.*;
import static org.awaitility.Awaitility.await;

/**
 * Integration tests that verify the complete event processing pipeline:
 * HTTP API → Kafka Producer → Embedded Kafka → Kafka Consumer → ClickHouse
 * 
 * This tests real application code with embedded Kafka and real ClickHouse.
 * Redis is mocked as it's an external dependency.
 */
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "spring.datasource.url=jdbc:clickhouse://localhost:8123/default",
        "spring.datasource.password=root"
    }
)
@AutoConfigureMockMvc
@EmbeddedKafka(
    partitions = 1,
    brokerProperties = {
        "log.dir=target/embedded-kafka"
    },
    topics = {
        "product_view", "cart_add", "cart_remove", "checkout_step", "purchase",
        "page_load", "page_view", "mouse_click", "button_click", "link_click",
        "form_focus", "form_input", "form_submit", "mouse_move", "scroll_depth",
        "video_Events", "custom_event", "file_download", "page_hidden", 
        "page_visible", "page_unload", "periodic_events"
    }
)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
// @DirtiesContext
public class RealIntegrationTest {

    @MockBean
    private RedisService redisService;

    @MockBean
    private RateLimiter rateLimiter;

    @Autowired
    private MockMvc mockMvc;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    private static final String TEST_TRACKING_ID = "integration-test-" + System.currentTimeMillis();

    @BeforeEach
    void setupMocks() {
        when(rateLimiter.rateLimiter(anyString())).thenReturn(true);
    }

    @BeforeAll
    void cleanupDatabase() {
        try {
            jdbcTemplate.update("TRUNCATE TABLE IF EXISTS page_loads");
            jdbcTemplate.update("TRUNCATE TABLE IF EXISTS page_events");
            jdbcTemplate.update("TRUNCATE TABLE IF EXISTS clicks");
            jdbcTemplate.update("TRUNCATE TABLE IF EXISTS interaction_events");
            jdbcTemplate.update("TRUNCATE TABLE IF EXISTS forms");
            jdbcTemplate.update("TRUNCATE TABLE IF EXISTS form_events");
            jdbcTemplate.update("TRUNCATE TABLE IF EXISTS ecommerce_events");
            jdbcTemplate.update("TRUNCATE TABLE IF EXISTS sessions");
            jdbcTemplate.update("TRUNCATE TABLE IF EXISTS session_pages");
        } catch (Exception e) {
            System.err.println("Cleanup error: " + e.getMessage());
        }
    }

    @Test
@Order(0)
void testKafkaConsumerIsRunning() {
    System.out.println("\n=== Verifying Kafka Consumer Status ===");
    
    // Check if the Kafka listener container is running
    try {
        // This will fail if Spring context didn't start the consumer
        assertNotNull(mockMvc, "MockMvc should be initialized");
        System.out.println("✓ Spring context loaded");
        
        // Try to verify embedded Kafka
        System.out.println("✓ Embedded Kafka should be running on: " + 
            System.getProperty("spring.embedded.kafka.brokers"));
        
    } catch (Exception e) {
        fail("Consumer verification failed: " + e.getMessage());
    }
}
    
    @Test
    @Order(1)
    void testHealthEndpoint() throws Exception {
        System.out.println("\n--- Test 1: Health Check ---");
        
        MvcResult result = mockMvc.perform(get("/health"))
            .andExpect(status().isOk())
            .andExpect(jsonPath("$.status").value("healthy"))
            .andReturn();
        
        System.out.println("✓ Health check passed: " + result.getResponse().getContentAsString());
    }

    @Test
    @Order(2)
    void testPageLoadEventToDatabase() throws Exception {
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

        MvcResult result = mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(eventData))
            .andExpect(status().isOk())
            .andReturn();

        System.out.println("✅ API Response: " + result.getResponse().getContentAsString());

        // Wait for Kafka to process and insert into ClickHouse
        await()
            .atMost(20, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM page_events WHERE tracking_id = ?",
                    Integer.class,
                    TEST_TRACKING_ID
                );
                System.out.println("✓ Found " + count + " page load events in ClickHouse");
                assertTrue(count > 0, "Page load event should be in database");
            });
    }

    @Test
    @Order(3)
    void testClickEventsToDatabase() throws Exception {
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

        MvcResult result = mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(clickData))
            .andExpect(status().isOk())
            .andReturn();

        System.out.println("✅ API Response: " + result.getResponse().getContentAsString());

        await()
            .atMost(20, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM interaction_events WHERE tracking_id = ?",
                    Integer.class,
                    TEST_TRACKING_ID
                );
                System.out.println("✓ Found " + count + " click events in ClickHouse");
                assertTrue(count >= 3, "Should have at least 3 click events, got: " + count);
            });
    }

    @Test
    @Order(4)
    void testFormEventsToDatabase() throws Exception {
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

        MvcResult result = mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(formData))
            .andExpect(status().isOk())
            .andReturn();

         System.out.println("✅ API Response: " + result.getResponse().getContentAsString());

        await()
            .atMost(20, TimeUnit.SECONDS)
            .pollInterval(1, TimeUnit.SECONDS)
            .untilAsserted(() -> {
                Integer count = jdbcTemplate.queryForObject(
                    "SELECT COUNT(*) FROM form_events WHERE tracking_id = ?",
                    Integer.class,
                    TEST_TRACKING_ID
                );
                System.out.println("✓ Found " + count + " form events in ClickHouse");
                assertTrue(count >= 2, "Should have at least 2 form events, got: " + count);
            });
    }

@Test
@Order(5)
void testEcommerceEventsToDatabase() throws Exception {
    System.out.println("\n========================================");
    System.out.println("Test 5: E-commerce Events End-to-End");
    System.out.println("========================================");
    
    String ecommerceData = String.format("""
[
  {
    "event_type": "purchase",
    "data": {
      "timestamp": "2026-01-16T12:00:00Z",
      "session_id": "test-session-001",
      "user_id": "1234",
      "tracking_id": "%s",
      "page_url": "https://example.com/checkout",
      "product_id": "PROD-101",
      "product_name": "Keyboard",
      "price": 49.99,
      "quantity": 1,
      "category": "Electronics",
      "currency": "USD",
      "order_id": "ORD-101",
      "total": 49.99,
      "step": 4,
      "step_name": "payment"
    }
  },
  {
    "event_type": "purchase",
    "data": {
      "timestamp": "2026-01-16T12:01:00Z",
      "session_id": "test-session-001",
      "user_id": "1234",
      "tracking_id": "%s",
      "page_url": "https://example.com/checkout",
      "product_id": "PROD-102",
      "product_name": "Mouse",
      "price": 29.99,
      "quantity": 2,
      "category": "Electronics",
      "currency": "USD",
      "order_id": "ORD-102",
      "total": 59.98,
      "step": 4,
      "step_name": "payment"
    }
  },
  {
    "event_type": "purchase",
    "data": {
      "timestamp": "2026-01-16T12:02:00Z",
      "session_id": "test-session-001",
      "user_id": "1234",
      "tracking_id": "%s",
      "page_url": "https://example.com/checkout",
      "product_id": "PROD-103",
      "product_name": "Headphones",
      "price": 89.99,
      "quantity": 1,
      "category": "Electronics",
      "currency": "USD",
      "order_id": "ORD-103",
      "total": 89.99,
      "step": 4,
      "step_name": "payment"
    }
  }
]
""", TEST_TRACKING_ID, TEST_TRACKING_ID, TEST_TRACKING_ID);

    System.out.println("\n📤 Sending e-commerce events...");
    System.out.println("Payload preview: " + ecommerceData.substring(0, Math.min(200, ecommerceData.length())) + "...");

    MvcResult result = mockMvc.perform(post("/receive_data")
            .contentType(MediaType.APPLICATION_JSON)
            .content(ecommerceData))
        .andExpect(status().isOk())
        .andReturn();

    String responseBody = result.getResponse().getContentAsString();
    System.out.println("\n✅ API Response: " + responseBody);
    
    // Parse response to check what happened
    if (responseBody.contains("skipped")) {
        System.out.println("⚠️  WARNING: Some events were skipped!");
    }

    System.out.println("\n⏳ Waiting for Kafka to process events...");
    
// First, just check if the consumer is working at all
await()
    .atMost(10, SECONDS)
    .untilAsserted(() -> {
        Integer count = jdbcTemplate.queryForObject(
            "SELECT count() FROM ecommerce_events",
            Integer.class
        );
        assertTrue(count > 0);
    });

// Check for our specific tracking ID
Integer ecommerceCount = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM ecommerce_events WHERE tracking_id = ?",
                Integer.class,
                TEST_TRACKING_ID
            );
            
            // Check total records in table
            Integer totalInAllTables = jdbcTemplate.queryForObject(
                "SELECT COUNT(*) FROM ecommerce_events",
                Integer.class
            );
            
            System.out.println("   📊 Events for this test (tracking_id=" + TEST_TRACKING_ID + "): " + ecommerceCount);
            System.out.println("   📊 Total events in ecommerce_events table: " + totalInAllTables);
            
            // If there are ANY records, print samples to debug
            if (totalInAllTables > 0) {
                System.out.println("\n   📋 Recent records in table:");
                List<Map<String, Object>> samples = jdbcTemplate.queryForList(
                    "SELECT event_type, tracking_id, product_id, session_id FROM ecommerce_events ORDER BY timestamp DESC LIMIT 5"
                );
                samples.forEach(row -> System.out.println("      " + row));
            } else {
                System.out.println("   ⚠️  Table is completely empty!");
                
                // Check if the table even exists
                try {
                    jdbcTemplate.queryForObject("SELECT 1 FROM ecommerce_events LIMIT 1", Integer.class);
                    System.out.println("   ✓ Table exists but has no data");
                } catch (Exception e) {
                    System.out.println("   ❌ Table might not exist or is not accessible: " + e.getMessage());
                }
            }
            
            assertTrue(ecommerceCount >= 3, 
                "Expected 3+ e-commerce events for tracking_id=" + TEST_TRACKING_ID + ", but got: " + ecommerceCount);
    
    System.out.println("\n✅ Test passed!");
    System.out.println("========================================\n");
}
    @Test
    @Order(6)
    void testDataIntegrityAcrossTables() {
        System.out.println("\n--- Test 6: Data Integrity Across All Tables ---");
        
        int pageEvents = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM page_events WHERE tracking_id = ?", 
            Integer.class, 
            TEST_TRACKING_ID
        );
        int interactionEvents = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM interaction_events WHERE tracking_id = ?", 
            Integer.class, 
            TEST_TRACKING_ID
        );
        int formEvents = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM form_events WHERE tracking_id = ?", 
            Integer.class, 
            TEST_TRACKING_ID
        );
        int ecommerceEvents = jdbcTemplate.queryForObject(
            "SELECT COUNT(*) FROM ecommerce_events WHERE tracking_id = ?", 
            Integer.class, 
            TEST_TRACKING_ID
        );
        
        int totalCount = pageEvents + interactionEvents + formEvents + ecommerceEvents;
        
        System.out.println("📊 Event Distribution:");
        System.out.println("   - Page Events: " + pageEvents);
        System.out.println("   - Interaction Events: " + interactionEvents);
        System.out.println("   - Form Events: " + formEvents);
        System.out.println("   - E-commerce Events: " + ecommerceEvents);
        System.out.println("   - Total: " + totalCount);
        
        assertTrue(totalCount > 0, "Should have data in at least one table");
        assertTrue(pageEvents > 0, "Should have page events");
        assertTrue(interactionEvents >= 3, "Should have click events");
        assertTrue(formEvents >= 2, "Should have form events");
        assertTrue(ecommerceEvents >= 3, "Should have e-commerce events");
    }

    @AfterAll
    void cleanup() {
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
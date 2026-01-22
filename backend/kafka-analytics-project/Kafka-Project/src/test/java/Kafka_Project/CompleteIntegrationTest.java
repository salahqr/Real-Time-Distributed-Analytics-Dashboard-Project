package Kafka_Project;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.http.MediaType;

import Kafka_Project.Redis.*;
import org.springframework.kafka.test.context.EmbeddedKafka;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.springframework.test.web.servlet.MvcResult;

import java.util.concurrent.TimeUnit;

import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;
import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(
    properties = "spring.profiles.active=test",
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@AutoConfigureMockMvc
@EmbeddedKafka(
    partitions = 1,
    topics = {
        "product_view", "cart_add", "cart_remove", "checkout_step", "purchase",
        "page_load", "page_view", "mouse_click", "button_click", "link_click",
        "form_focus", "form_input", "form_submit", "mouse_move", "scroll_depth",
        "video_Events", "custom_event", "file_download", "page_hidden", 
        "page_visible", "page_unload", "periodic_events"
    }
)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CompleteIntegrationTest {

    // @MockBean
    // private KafkaProducerService kafkaProducerService;
    
    @MockBean
    private RedisService redisService; 

    @MockBean
    private RateLimiter rateLimiter;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MockMvc mockMvc;
// In ClickHouseDatabaseTest.java

@BeforeEach
void setup() {
    // Setup test user
    when(rateLimiter.rateLimiter(anyString())).thenReturn(true);

    String makeUser = 
        "INSERT INTO user (user_id, company_name, email, password, is_verify) " +
        "VALUES (1234, 'test_company', 'test@example.com', 'password123', 1)";

    try {
    } catch (Exception e) {
        // User might already exist
    }
    
    // Clean up ALL tables before each test
    try {
        jdbcTemplate.update("TRUNCATE TABLE ecommerce_events");
        jdbcTemplate.update("TRUNCATE TABLE sessions");
        jdbcTemplate.update("TRUNCATE TABLE session_pages");
        jdbcTemplate.update("TRUNCATE TABLE user_first_session");
        jdbcTemplate.update("TRUNCATE TABLE mv_traffic_5m");
    } catch (Exception e) {
        System.err.println("Error truncating tables: " + e.getMessage());
    }
}

@AfterEach
void cleanup() {
    try {
        jdbcTemplate.update("TRUNCATE TABLE ecommerce_events");
        jdbcTemplate.update("TRUNCATE TABLE sessions");
        jdbcTemplate.update("TRUNCATE TABLE session_pages");
        jdbcTemplate.update("TRUNCATE TABLE user_first_session");
        jdbcTemplate.update("TRUNCATE TABLE mv_traffic_5m");
    } catch (Exception e) {
        System.err.println("Error in cleanup: " + e.getMessage());
    }
}

    @org.junit.jupiter.api.BeforeEach
    void setupMocks() {
        // Mock rate limiter to always allow requests before each test
        when(rateLimiter.rateLimiter(anyString())).thenReturn(true);
    }

    @Test
    void testHealthCheck() throws Exception {
        mockMvc.perform(get("/health"))
               .andExpect(status().isOk())
               .andExpect(jsonPath("$.status").value("healthy"));
    }

    @Test
    void testReadyCheck() throws Exception {
        mockMvc.perform(get("/ready"))
               .andExpect(status().isOk())
               .andExpect(jsonPath("$.kafka").exists())
               .andExpect(jsonPath("$.redis").exists())
               .andExpect(jsonPath("$.clickhouse").exists());
    }

    @Test
    void testPageLoadEvent() throws Exception {
        String pageLoadData = """
        [
            {
                "type": "page_load",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "url": "https://example.com/home",
                    "referrer": "https://google.com",
                    "title": "Home Page",
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
                    },
                    "network": {
                        "effectiveType": "4g",
                        "downlink": 10,
                        "rtt": 50,
                        "saveData": false
                    }
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(pageLoadData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.status").value("success"))
                .andExpect(jsonPath("$.processed").value(1));
    }

    @Test
    void testPageViewEvent() throws Exception {
        String pageViewData = """
        [
            {
                "type": "page_view",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/products",
                    "page_title": "Products Page",
                    "referrer": "https://example.com/home",
                    "duration_ms": 5000,
                    "scroll_depth_max": 75.5,
                    "click_count": 3
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(pageViewData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(1));
    }

    @Test
    void testClickEvents() throws Exception {
        String clickData = """
        [
            {
                "type": "mouse_click",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/products",
                    "x": 450,
                    "y": 300,
                    "element": "button",
                    "element_id": "buy-now",
                    "element_class": "btn btn-primary"
                }
            },
            {
                "type": "button_click",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/products",
                    "button_text": "Add to Cart",
                    "button_type": "submit",
                    "button_id": "add-cart-btn"
                }
            },
            {
                "type": "link_click",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/products",
                    "link_url": "https://example.com/product/123",
                    "link_text": "View Details",
                    "is_external": false,
                    "target": "_self"
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(clickData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(3));
    }

    @Test
    void testMouseMovementEvents() throws Exception {
        String mouseData = """
        [
            {
                "type": "mouse_move",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/products",
                    "x": 100,
                    "y": 200
                }
            },
            {
                "type": "mouse_move",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/products",
                    "x": 150,
                    "y": 250
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(mouseData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(2));
    }

    @Test
    void testScrollEvents() throws Exception {
        String scrollData = """
        [
            {
                "type": "scroll_depth",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/article",
                    "depth_percent": 25,
                    "scroll_top": 500,
                    "scroll_percent": 25
                }
            },
            {
                "type": "scroll_depth",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/article",
                    "depth_percent": 50,
                    "scroll_top": 1000,
                    "scroll_percent": 50
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(scrollData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(2));
    }

    @Test
    void testFormEvents() throws Exception {
        String formData = """
        [
            {
                "type": "form_focus",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/contact",
                    "form_id": "contact-form",
                    "form_name": "contact",
                    "field_name": "email",
                    "field_type": "email"
                }
            },
            {
                "type": "form_input",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/contact",
                    "form_id": "contact-form",
                    "field_name": "email",
                    "field_type": "email",
                    "value_length": 25
                }
            },
            {
                "type": "form_submit",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/contact",
                    "form_id": "contact-form",
                    "form_name": "contact",
                    "form_action": "/api/contact",
                    "form_method": "POST",
                    "field_count": 5,
                    "has_file_upload": false,
                    "success": true
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(formData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(3));
    }

    @Test
    void testVideoEvents() throws Exception {
        String videoData = """
        [
            {
                "type": "video_Events",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/video",
                    "event_type": "play",
                    "video_src": "https://example.com/videos/demo.mp4",
                    "video_duration": 120.5,
                    "current_time": 0.0
                }
            },
            {
                "type": "video_Events",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/video",
                    "event_type": "progress_25",
                    "video_src": "https://example.com/videos/demo.mp4",
                    "video_duration": 120.5,
                    "current_time": 30.125
                }
            },
            {
                "type": "video_Events",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/video",
                    "event_type": "complete",
                    "video_src": "https://example.com/videos/demo.mp4",
                    "video_duration": 120.5,
                    "current_time": 120.5
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(videoData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(3));
    }
    
    @Test
void testEcommerceEvents_KafkaToClickHouse_DEFAULT_SCHEMA() throws Exception {

    // 0️⃣ Clean previous data
    jdbcTemplate.update(
        "ALTER TABLE default.ecommerce_events DELETE WHERE session_id = ?",
        "test-session-001"
    );

    jdbcTemplate.update(
        "ALTER TABLE default.sessions DELETE WHERE session_id = ?",
        "test-session-001"
    );

    // 2️⃣ Insert SESSION
    jdbcTemplate.update(
        "INSERT INTO default.sessions " +
        "(session_id, user_id, tracking_id, start_time, device_type, operating_system, browser, created_at, page_views, duration_ms, bounce) " +
        "VALUES (?, ?, ?, ?, 'Desktop', 'Unknown', 'Unknown', now(), 1, 0, 0)",
        "test-session-001",
        "1234",
        "test-tracking-001",
        "2026-01-14 20:00:00"
    );

    // 3️⃣ Payload (exactly like Postman)
    String payload = """
    [
      {
        "event_type": "purchase",
        "data": {
          "timestamp": "2026-01-16T12:00:00Z",
          "session_id": "test-session-001",
          "user_id": "1234",
          "tracking_id": "test-tracking-001",
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
          "tracking_id": "test-tracking-001",
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
          "tracking_id": "test-tracking-001",
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
    """;

    // 4️⃣ Send request
    MvcResult result = mockMvc.perform(post("/receive_data")
            .contentType(MediaType.APPLICATION_JSON)
            .content(payload))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.processed").value(3))
        .andExpect(jsonPath("$.skipped").value(0))
        .andReturn();
    
    // 🔍 ADD THIS - Print the response to verify
    System.out.println("✅ Response: " + result.getResponse().getContentAsString());

    // 5️⃣ WAIT for Kafka consumer → ClickHouse
    Awaitility.await()
        .atMost(20, TimeUnit.SECONDS)
        .pollInterval(500, TimeUnit.MILLISECONDS)
        .untilAsserted(() -> {
            Integer count = jdbcTemplate.queryForObject(
                "SELECT count(*) FROM default.ecommerce_events WHERE session_id = ?",
                Integer.class,
                "test-session-001"
            );
            System.out.println("🔍 Current count in DB: " + count); // ADD THIS
            assertEquals(3, count);
        });
}


    @Test
    void testCustomEvent() throws Exception {
        String customData = """
        [
            {
                "type": "custom_event",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/app",
                    "event_name": "feature_used",
                    "properties": {
                        "feature": "advanced_search",
                        "query": "wireless headphones",
                        "filters": ["electronics", "in-stock"],
                        "result_count": 42
                    }
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(customData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(1));
    }

    @Test
    void testFileDownloadEvent() throws Exception {
        String downloadData = """
        [
            {
                "type": "file_download",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/resources",
                    "link_url": "https://example.com/files/whitepaper.pdf",
                    "file_name": "whitepaper.pdf",
                    "link_text": "Download Whitepaper",
                    "is_external": false
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(downloadData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(1));
    }

    @Test
    void testPageVisibilityEvents() throws Exception {
        String visibilityData = """
        [
            {
                "type": "page_hidden",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/article"
                }
            },
            {
                "type": "page_visible",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/article"
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(visibilityData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(2));
    }

    @Test
    void testPageUnloadEvent() throws Exception {
        String unloadData = """
        [
            {
                "type": "page_unload",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/article",
                    "duration_ms": 45000,
                    "scroll_depth_max": 85.5,
                    "click_count": 7
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(unloadData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(1));
    }

    @Test
    void testPeriodicBatchEvent() throws Exception {
        String periodicData = """
        [
            {
                "type": "periodic_events",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "clickCount": 5,
                    "linkClicks": [
                        {
                            "url": "https://example.com/page1",
                            "text": "Link 1",
                            "ts": 1638360000000
                        }
                    ],
                    "mouseClicks": [
                        {
                            "x": 100,
                            "y": 200,
                            "element": "button",
                            "ts": 1638360001000
                        }
                    ],
                    "scrollEvents": [
                        {
                            "scroll_percent": 50,
                            "scroll_top": 1000,
                            "ts": 1638360002000
                        }
                    ]
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(periodicData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(1));
    }

    @Test
    void testMultipleEventsBatch() throws Exception {
        String batchData = """
        [
            {
                "type": "page_view",
                "data": {
                    "session_id": "test-session-002",
                    "user_id": "test-user-002",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/home",
                    "page_title": "Home"
                }
            },
            {
                "type": "mouse_click",
                "data": {
                    "session_id": "test-session-002",
                    "user_id": "test-user-002",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/home",
                    "x": 200,
                    "y": 300,
                    "element": "a"
                }
            },
            {
                "type": "product_view",
                "data": {
                    "session_id": "test-session-002",
                    "user_id": "test-user-002",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/product/456",
                    "product_id": "PROD-456",
                    "product_name": "Laptop",
                    "price": 1299.99,
                    "category": "Computers"
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(batchData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(3))
                .andExpect(jsonPath("$.skipped").value(0));
    }

    @Test
    void testInvalidEventType() throws Exception {
        String invalidData = """
        [
            {
                "type": "invalid_event_type",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001"
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(invalidData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(0))
                .andExpect(jsonPath("$.skipped").value(1));
    }

    @Test
    void testMissingDataField() throws Exception {
        String missingDataField = """
        [
            {
                "type": "page_view"
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(missingDataField))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(0))
                .andExpect(jsonPath("$.skipped").value(1));
    }

    @Test
    void testRateLimitExceeded() throws Exception {
        // Mock rate limiter to deny request
        when(rateLimiter.rateLimiter(anyString())).thenReturn(false);

        String testData = """
        [
            {
                "type": "page_view",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/home"
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(testData))
                .andExpect(status().isTooManyRequests())
                .andExpect(jsonPath("$.error").value("Too many requests, try again later"));

        // Reset for other tests
        when(rateLimiter.rateLimiter(anyString())).thenReturn(true);
    }
}
package Kafka_Project;

import org.junit.jupiter.api.BeforeAll;
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
import Kafka_Project.service.KafkaProducerService;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@SpringBootTest(
    properties = "spring.profiles.active=test",
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT
)
@AutoConfigureMockMvc
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class CompleteIntegrationTest {

    @MockBean
    private KafkaProducerService kafkaProducerService;
    
    @MockBean
    private RedisService redisService; 

    @MockBean
    private RateLimiter rateLimiter;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Autowired
    private MockMvc mockMvc;

    @BeforeAll
    void setup() {
        // Setup test user
        String makeUser = 
            "INSERT INTO user (user_id, company_name, email, password, is_verify) " +
            "VALUES (1234, 'test_company', 'test@example.com', 'password123', 1)";
        
        try {
            jdbcTemplate.update(makeUser);
        } catch (Exception e) {
            // User might already exist
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
    void testEcommerceEvents() throws Exception {
        String ecommerceData = """
        [
            {
                "type": "product_view",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/product/123",
                    "product_id": "PROD-123",
                    "product_name": "Wireless Headphones",
                    "price": 99.99,
                    "category": "Electronics",
                    "currency": "USD"
                }
            },
            {
                "type": "cart_add",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/product/123",
                    "product_id": "PROD-123",
                    "product_name": "Wireless Headphones",
                    "price": 99.99,
                    "quantity": 1,
                    "category": "Electronics",
                    "currency": "USD"
                }
            },
            {
                "type": "checkout_step",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/checkout",
                    "step": 1,
                    "step_name": "Shipping Information"
                }
            },
            {
                "type": "purchase",
                "data": {
                    "session_id": "test-session-001",
                    "user_id": "test-user-001",
                    "tracking_id": "test-tracking-001",
                    "page_url": "https://example.com/order-confirmation",
                    "order_id": "ORD-2024-001",
                    "product_id": "PROD-123",
                    "product_name": "Wireless Headphones",
                    "price": 99.99,
                    "quantity": 1,
                    "total": 109.99,
                    "currency": "USD"
                }
            }
        ]
        """;

        mockMvc.perform(post("/receive_data")
                .contentType(MediaType.APPLICATION_JSON)
                .content(ecommerceData))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.processed").value(4));
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
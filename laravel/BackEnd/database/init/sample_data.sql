
INSERT INTO sessions (session_id, user_id, tracking_id, start_time, end_time, device_type, operating_system, browser, screen_width, screen_height, country, country_code, referrer, entry_page, exit_page, duration_ms, bounce, page_views) VALUES
('sess_001', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', now() - INTERVAL 2 HOUR, now() - INTERVAL 1 HOUR, 'desktop', 'Windows', 'Chrome', 1920, 1080, 'Jordan', 'JO', 'https://google.com', '/home', '/about', 180000, 0, 5),
('sess_002', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', now() - INTERVAL 5 HOUR, now() - INTERVAL 4 HOUR, 'mobile', 'iOS', 'Safari', 375, 812, 'Jordan', 'JO', 'direct', '/products', '/cart', 240000, 0, 8),
('sess_003', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', now() - INTERVAL 1 DAY, now() - INTERVAL 1 DAY + INTERVAL 30 MINUTE, 'desktop', 'macOS', 'Chrome', 1440, 900, 'United States', 'US', 'https://facebook.com', '/home', '/home', 5000, 1, 1),
('sess_004', 'user_002', 'site_default', now() - INTERVAL 3 HOUR, NULL, 'mobile', 'Android', 'Chrome', 360, 640, 'Jordan', 'JO', 'direct', '/blog', NULL, NULL, 0, 3),
('sess_005', 'user_003', 'site_default', now() - INTERVAL 6 HOUR, now() - INTERVAL 5 HOUR, 'desktop', 'Linux', 'Firefox', 1920, 1080, 'Germany', 'DE', 'https://twitter.com', '/features', '/pricing', 300000, 0, 10);

INSERT INTO page_events (timestamp, session_id, user_id, tracking_id, event_type, page_url, page_title, referrer, duration_ms, scroll_depth_max, click_count, page_load_time) VALUES
(now() - INTERVAL 2 HOUR, 'sess_001', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', 'page_view', '/home', 'Home Page', 'https://google.com', 45000, 85.5, 5, 1200),
(now() - INTERVAL 2 HOUR + INTERVAL 1 MINUTE, 'sess_001', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', 'page_view', '/products', 'Products', '/home', 60000, 92.3, 8, 980),
(now() - INTERVAL 2 HOUR + INTERVAL 2 MINUTE, 'sess_001', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', 'page_view', '/about', 'About Us', '/products', 30000, 70.2, 2, 850),
(now() - INTERVAL 5 HOUR, 'sess_002', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', 'page_view', '/products', 'Products', 'direct', 50000, 88.0, 12, 1100),
(now() - INTERVAL 5 HOUR + INTERVAL 1 MINUTE, 'sess_002', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', 'page_view', '/cart', 'Shopping Cart', '/products', 80000, 95.5, 15, 920),
(now() - INTERVAL 1 DAY, 'sess_003', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', 'page_view', '/home', 'Home Page', 'https://facebook.com', 5000, 15.0, 0, 1500),
(now() - INTERVAL 3 HOUR, 'sess_004', 'user_002', 'site_default', 'page_view', '/blog', 'Blog', 'direct', 90000, 98.0, 6, 1050),
(now() - INTERVAL 6 HOUR, 'sess_005', 'user_003', 'site_default', 'page_view', '/features', 'Features', 'https://twitter.com', 120000, 100.0, 20, 890);

INSERT INTO interaction_events (timestamp, session_id, user_id, tracking_id, event_type, page_url, element, button_text, link_url) VALUES
(now() - INTERVAL 2 HOUR, 'sess_001', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', 'button_click', '/home', 'button', 'Get Started', NULL),
(now() - INTERVAL 2 HOUR + INTERVAL 30 SECOND, 'sess_001', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', 'link_click', '/products', 'a', 'Learn More', '/about'),
(now() - INTERVAL 5 HOUR, 'sess_002', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', 'button_click', '/products', 'button', 'Add to Cart', NULL);

INSERT INTO form_events (timestamp, session_id, user_id, tracking_id, page_url, event_type, form_id, form_name, success) VALUES
(now() - INTERVAL 3 HOUR, 'sess_004', 'user_002', 'site_default', '/contact', 'form_focus', 'contact_form', 'Contact Form', NULL),
(now() - INTERVAL 3 HOUR + INTERVAL 2 MINUTE, 'sess_004', 'user_002', 'site_default', '/contact', 'form_submit', 'contact_form', 'Contact Form', 1);

INSERT INTO ecommerce_events (timestamp, session_id, user_id, tracking_id, page_url, event_type, product_id, product_name, price, quantity, category, order_id, total) VALUES
(now() - INTERVAL 5 HOUR, 'sess_002', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', '/products', 'product_view', 'prod_001', 'Laptop', 999.99, NULL, 'Electronics', NULL, NULL),
(now() - INTERVAL 5 HOUR + INTERVAL 1 MINUTE, 'sess_002', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', '/cart', 'cart_add', 'prod_001', 'Laptop', 999.99, 1, 'Electronics', NULL, NULL),
(now() - INTERVAL 5 HOUR + INTERVAL 2 MINUTE, 'sess_002', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', '/checkout', 'checkout_step', 'prod_001', 'Laptop', 999.99, 1, 'Electronics', NULL, NULL),
(now() - INTERVAL 5 HOUR + INTERVAL 3 MINUTE, 'sess_002', '58179503-3474-45a3-8851-c8f49449be5e', 'site_default', '/success', 'purchase', 'prod_001', 'Laptop', 999.99, 1, 'Electronics', 'order_001', 999.99);

SELECT 'Sessions:', count() FROM sessions;
SELECT 'Page Events:', count() FROM page_events;
SELECT 'Interaction Events:', count() FROM interaction_events;
SELECT 'Form Events:', count() FROM form_events;
SELECT 'Ecommerce Events:', count() FROM ecommerce_events;
CREATE TABLE users (
    user_id String,
    company_name String,
    email String,
    password String,
    is_verify UInt8,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY user_id;

CREATE TABLE sessions (
    session_id String,
    user_id String,
    start_time DateTime DEFAULT now(),
    end_time DateTime DEFAULT now(),
    country String,
    device String, -- 'mobile', 'desktop', 'tablet'
    browser String,
    is_active UInt8 DEFAULT 1
) ENGINE = MergeTree()
ORDER BY start_time;

CREATE TABLE page_views (
    session_id String,
    page_url String,
    page_title String,
    timestamp DateTime DEFAULT now(),
    time_on_page UInt32 DEFAULT 0
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE events (
    session_id String,
    event_type String, -- 'click', 'form_submit', 'download'
    element_info String,
    page_url String,
    timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY timestamp;

CREATE TABLE ecommerce_events (
    session_id String,
    event_type String, -- 'product_view', 'add_to_cart', 'purchase'
    product_id String,
    product_name String,
    price Float32,
    timestamp DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY timestamp;

-- =====================================================
-- AGGREGATED STATS TABLES
-- =====================================================

-- 5-minute stats
CREATE TABLE stats_5min (
    time_interval DateTime, -- rounded to 5-min (like 14:05:00, 14:10:00)
    active_users UInt32,
    total_pageviews UInt32,
    total_clicks UInt32,
    mobile_users UInt32,
    desktop_users UInt32,
    top_pages String -- JSON: [{"url":"/home","views":100}]
) ENGINE = SummingMergeTree()
ORDER BY time_interval;

-- 1-hour stats  
CREATE TABLE stats_1hour (
    time_interval DateTime, -- rounded to hour (like 14:00:00, 15:00:00)
    active_users UInt32,
    total_pageviews UInt32,
    total_clicks UInt32,
    total_purchases UInt32,
    revenue Float32,
    mobile_users UInt32,
    desktop_users UInt32,
    top_countries String -- JSON: [{"country":"US","users":50}]
) ENGINE = SummingMergeTree()
ORDER BY time_interval;

-- 1-day stats
CREATE TABLE stats_1day (
    date Date,
    active_users UInt32,
    total_pageviews UInt32,
    total_clicks UInt32,
    total_purchases UInt32,
    revenue Float32,
    mobile_users UInt32,
    desktop_users UInt32,
    bounce_rate Float32,
    avg_session_time Float32
) ENGINE = SummingMergeTree()
ORDER BY date;

-- Real-time current stats (updated every 5 seconds)
CREATE TABLE realtime_current (
    timestamp DateTime,
    online_users UInt32,
    current_pageviews UInt32,
    popular_pages String, -- JSON array
    device_breakdown String -- JSON: {"mobile":20,"desktop":30}
) ENGINE = ReplacingMergeTree()
ORDER BY timestamp;

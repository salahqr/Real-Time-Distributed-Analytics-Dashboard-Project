-- =====================================================
-- ANALYTICS DATABASE SCHEMA (Fixed Version)
-- =====================================================

-- Users table for tracking system owners
CREATE TABLE users (
    user_id String,
    company_name String,
    email String,
    password String,
    is_verify UInt8,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY user_id;

-- Sessions table (updated to match JS data structure)
CREATE TABLE sessions (
    session_id String,
    user_id String,
    tracking_id String,
    start_time DateTime,
    end_time Nullable(DateTime),
    device_type String,
    operating_system String,
    browser String,
    screen_width UInt16,
    screen_height UInt16,
    viewport_width UInt16,
    viewport_height UInt16,
    country Nullable(String),
    country_code Nullable(String),
    language String,
    timezone String,
    referrer String,
    entry_page String,
    exit_page Nullable(String),
    duration_ms Nullable(UInt32),
    bounce UInt8 DEFAULT 0,
    page_views UInt16 DEFAULT 0,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(start_time)
ORDER BY (tracking_id, session_id, start_time);

-- Page events (page loads, views, unloads)
CREATE TABLE page_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    event_type Enum8('page_load' = 1, 'page_view' = 2, 'page_unload' = 3, 'page_hidden' = 4, 'page_visible' = 5),
    page_url String,
    page_title String,
    referrer String,
    duration_ms Nullable(UInt32),
    scroll_depth_max Nullable(Float32),
    click_count Nullable(UInt16),
    -- Performance data
    dns_time Nullable(UInt16),
    connect_time Nullable(UInt16),
    response_time Nullable(UInt16),
    dom_load_time Nullable(UInt16),
    page_load_time Nullable(UInt16),
    -- Network info
    connection_type Nullable(String),
    connection_downlink Nullable(Float32),
    connection_rtt Nullable(UInt16),
    save_data Nullable(UInt8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

-- Click and interaction events
CREATE TABLE interaction_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    event_type Enum8('mouse_click' = 1, 'button_click' = 2, 'link_click' = 3, 'file_download' = 4),
    page_url String,
    x Nullable(UInt16),
    y Nullable(UInt16),
    element String,
    element_id Nullable(String),
    element_class Nullable(String),
    button_text Nullable(String),
    button_type Nullable(String),
    link_url Nullable(String),
    link_text Nullable(String),
    file_name Nullable(String),
    is_external Nullable(UInt8),
    target Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

-- Mouse movement events (sampled)
CREATE TABLE mouse_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    page_url String,
    x UInt16,
    y UInt16
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp)
SETTINGS index_granularity = 8192;

-- Scroll events
CREATE TABLE scroll_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    page_url String,
    event_type Enum8('scroll_depth' = 1, 'scroll_position' = 2),
    depth_percent Nullable(UInt8), -- for milestone tracking
    scroll_top Nullable(UInt16),
    scroll_percent Nullable(UInt8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

-- Form events
CREATE TABLE form_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    page_url String,
    event_type Enum8('form_focus' = 1, 'form_input' = 2, 'form_submit' = 3),
    form_id String,
    form_name Nullable(String),
    form_action Nullable(String),
    form_method Nullable(String),
    field_name Nullable(String),
    field_type Nullable(String),
    field_count Nullable(UInt8),
    value_length Nullable(UInt16),
    has_file_upload Nullable(UInt8),
    success Nullable(UInt8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

-- Video events
CREATE TABLE video_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    page_url String,
    event_type Enum8('play' = 1, 'pause' = 2, 'complete' = 3, 'progress_25' = 4, 'progress_50' = 5, 'progress_75' = 6),
    video_src String,
    video_duration Nullable(Float32),
    current_time Nullable(Float32)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

-- E-commerce events
CREATE TABLE ecommerce_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    page_url String,
    event_type Enum8('product_view' = 1, 'cart_add' = 2, 'cart_remove' = 3, 'checkout_step' = 4, 'purchase' = 5),
    product_id Nullable(String),
    product_name Nullable(String),
    price Nullable(Float32),
    quantity Nullable(UInt16),
    category Nullable(String),
    currency String DEFAULT 'USD',
    order_id Nullable(String),
    total Nullable(Float32),
    step Nullable(UInt8),
    step_name Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

-- Custom events
CREATE TABLE custom_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    page_url String,
    event_name String,
    properties String -- JSON string
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

-- Batch/periodic events (for buffered data)
CREATE TABLE batch_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    event_data String, -- JSON string containing batched events
    event_count UInt16
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

-- =====================================================
-- REAL-TIME AGGREGATION TABLES
-- =====================================================

-- Real-time stats (5-second intervals)
CREATE TABLE realtime_stats (
    timestamp DateTime,
    tracking_id String,
    online_users UInt32,
    page_views UInt32,
    clicks UInt32,
    form_submissions UInt32,
    video_plays UInt32,
    cart_actions UInt32,
    purchases UInt32
) ENGINE = SummingMergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, timestamp)
SETTINGS index_granularity = 8192;

-- Hourly aggregated stats
CREATE TABLE hourly_stats (
    hour DateTime,
    tracking_id String,
    unique_users AggregateFunction(uniq, String),
    total_sessions UInt32,
    total_pageviews UInt32,
    total_clicks UInt32,
    total_form_submissions UInt32,
    total_purchases UInt32,
    total_revenue AggregateFunction(sum, Float32),
    mobile_users AggregateFunction(uniq, String),
    desktop_users AggregateFunction(uniq, String),
    tablet_users AggregateFunction(uniq, String),
    bounce_rate AggregateFunction(avg, Float32),
    avg_session_duration AggregateFunction(avg, UInt32)
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(hour)
ORDER BY (tracking_id, hour);

-- Daily aggregated stats
CREATE TABLE daily_stats (
    date Date,
    tracking_id String,
    unique_users AggregateFunction(uniq, String),
    total_sessions UInt32,
    total_pageviews UInt32,
    total_clicks UInt32,
    total_form_submissions UInt32,
    total_purchases UInt32,
    total_revenue AggregateFunction(sum, Float32),
    mobile_users AggregateFunction(uniq, String),
    desktop_users AggregateFunction(uniq, String),
    tablet_users AggregateFunction(uniq, String),
    bounce_rate AggregateFunction(avg, Float32),
    avg_session_duration AggregateFunction(avg, UInt32),
    top_pages String, -- JSON
    top_countries String -- JSON
) ENGINE = AggregatingMergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (tracking_id, date);

-- =====================================================
-- MATERIALIZED VIEWS FOR AUTO-AGGREGATION
-- =====================================================

-- Real-time stats materialized view
CREATE MATERIALIZED VIEW realtime_stats_mv TO realtime_stats AS
SELECT
    toStartOfInterval(timestamp, INTERVAL 5 SECOND) as timestamp,
    tracking_id,
    0 as online_users, -- Will be updated by separate process
    countIf(event_type = 'page_view') as page_views,
    0 as clicks,
    0 as form_submissions,
    0 as video_plays,
    0 as cart_actions,
    0 as purchases
FROM page_events
GROUP BY timestamp, tracking_id;

-- Hourly stats materialized view
CREATE MATERIALIZED VIEW hourly_stats_mv TO hourly_stats AS
SELECT
    toStartOfHour(timestamp) as hour,
    tracking_id,
    uniqState(user_id) as unique_users,
    0 as total_sessions,
    countIf(event_type = 'page_view') as total_pageviews,
    0 as total_clicks,
    0 as total_form_submissions,
    0 as total_purchases,
    sumState(toFloat32(0.0)) as total_revenue,
    uniqState('') as mobile_users,
    uniqState('') as desktop_users, 
    uniqState('') as tablet_users,
    avgState(toFloat32(0.0)) as bounce_rate,
    avgState(toUInt32(0)) as avg_session_duration
FROM page_events
GROUP BY hour, tracking_id;

-- =====================================================
-- INDEXES FOR BETTER PERFORMANCE
-- =====================================================

-- Add indexes for common queries
ALTER TABLE page_events ADD INDEX idx_page_url page_url TYPE bloom_filter GRANULARITY 1;
ALTER TABLE interaction_events ADD INDEX idx_element element TYPE bloom_filter GRANULARITY 1;
ALTER TABLE form_events ADD INDEX idx_form_id form_id TYPE bloom_filter GRANULARITY 1;
ALTER TABLE ecommerce_events ADD INDEX idx_product_id product_id TYPE bloom_filter GRANULARITY 1;

-- =====================================================
-- TTL POLICIES FOR DATA RETENTION
-- =====================================================

-- Set TTL for raw events (keep for 1 year)
ALTER TABLE page_events MODIFY TTL timestamp + INTERVAL 1 YEAR;
ALTER TABLE interaction_events MODIFY TTL timestamp + INTERVAL 1 YEAR;
ALTER TABLE mouse_events MODIFY TTL timestamp + INTERVAL 3 MONTH; -- Shorter for high-volume data
ALTER TABLE scroll_events MODIFY TTL timestamp + INTERVAL 6 MONTH;
ALTER TABLE form_events MODIFY TTL timestamp + INTERVAL 1 YEAR;
ALTER TABLE video_events MODIFY TTL timestamp + INTERVAL 1 YEAR;
ALTER TABLE custom_events MODIFY TTL timestamp + INTERVAL 1 YEAR;
ALTER TABLE batch_events MODIFY TTL timestamp + INTERVAL 6 MONTH;

-- Keep aggregated data longer
ALTER TABLE realtime_stats MODIFY TTL timestamp + INTERVAL 1 MONTH;
ALTER TABLE hourly_stats MODIFY TTL hour + INTERVAL 2 YEAR;
ALTER TABLE daily_stats MODIFY TTL date + INTERVAL 5 YEAR;
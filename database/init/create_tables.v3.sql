-- =====================================================
-- FIXED CLICKHOUSE SCHEMA - Addresses Code 349 Errors
-- =====================================================

-- Drop all existing views first
DROP VIEW IF EXISTS mv_conversion_funnel;
DROP VIEW IF EXISTS mv_user_first_session;
DROP VIEW IF EXISTS mv_traffic_5m;
DROP VIEW IF EXISTS mv_traffic_1h;
DROP VIEW IF EXISTS mv_traffic_1d;
DROP VIEW IF EXISTS mv_page_1h;
DROP VIEW IF EXISTS mv_page_1d;
DROP VIEW IF EXISTS mv_device_1h;
DROP VIEW IF EXISTS mv_device_1d;
DROP VIEW IF EXISTS mv_geo_1h;
DROP VIEW IF EXISTS mv_geo_1d;
DROP VIEW IF EXISTS mv_source_1h;
DROP VIEW IF EXISTS mv_source_1d;
DROP VIEW IF EXISTS mv_interaction;
DROP VIEW IF EXISTS mv_interaction_1d;
DROP VIEW IF EXISTS mv_form;
DROP VIEW IF EXISTS mv_form_1d;
DROP VIEW IF EXISTS mv_ecommerce;
DROP VIEW IF EXISTS mv_ecommerce_1d;
DROP VIEW IF EXISTS mv_product;
DROP VIEW IF EXISTS mv_video;
DROP VIEW IF EXISTS mv_session_pages;

-- =====================================================
-- CRITICAL FIX: Drop and recreate tables with REQUIRED fields
-- =====================================================

-- These tables have missing required fields causing Code 349 errors
DROP TABLE IF EXISTS page_events;
DROP TABLE IF EXISTS form_events;
DROP TABLE IF EXISTS ecommerce_events;
DROP TABLE IF EXISTS sessions;

-- 1. sessions - FIXED: Added ALL required fields with defaults
CREATE TABLE IF NOT EXISTS sessions (
    session_id String,
    user_id String,
    tracking_id String,
    start_time DateTime,
    end_time Nullable(DateTime),
    device_type LowCardinality(String) DEFAULT 'Unknown',
    operating_system LowCardinality(String) DEFAULT 'Unknown',
    browser LowCardinality(String) DEFAULT 'Unknown',
    screen_width UInt16 DEFAULT 0,
    screen_height UInt16 DEFAULT 0,
    viewport_width UInt16 DEFAULT 0,
    viewport_height UInt16 DEFAULT 0,
    country LowCardinality(Nullable(String)),
    country_code LowCardinality(Nullable(String)),
    language LowCardinality(String) DEFAULT 'en',
    timezone String DEFAULT 'UTC',
    referrer String DEFAULT '',
    entry_page String DEFAULT '',
    exit_page Nullable(String),
    duration_ms Nullable(UInt32),
    bounce UInt8 DEFAULT 0,
    page_views UInt16 DEFAULT 0,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(start_time)
ORDER BY (tracking_id, session_id, start_time);

-- 2. page_events - FIXED: Removed enum, use String instead
CREATE TABLE IF NOT EXISTS page_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    event_type String, -- CHANGED FROM ENUM
    page_url String,
    page_title String DEFAULT '',
    referrer String DEFAULT '',
    duration_ms Nullable(UInt32),
    scroll_depth_max Nullable(Float32),
    click_count Nullable(UInt16),
    dns_time Nullable(UInt16),
    connect_time Nullable(UInt16),
    response_time Nullable(UInt16),
    dom_load_time Nullable(UInt16),
    page_load_time Nullable(UInt16),
    connection_type LowCardinality(Nullable(String)),
    connection_downlink Nullable(Float32),
    connection_rtt Nullable(UInt16),
    save_data Nullable(UInt8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

-- 3. form_events - FIXED: Removed enum
CREATE TABLE IF NOT EXISTS form_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    page_url String,
    event_type String, -- CHANGED FROM ENUM
    form_id String,
    form_name String DEFAULT 'default_form',
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

-- 4. ecommerce_events - FIXED: Removed enum
CREATE TABLE IF NOT EXISTS ecommerce_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    page_url String,
    event_type String, -- CHANGED FROM ENUM
    product_id Nullable(String),
    product_name Nullable(String),
    price Nullable(Float64),
    quantity Nullable(UInt16),
    category LowCardinality(Nullable(String)),
    currency LowCardinality(Nullable(String)) DEFAULT 'USD',
    order_id Nullable(String),
    total Nullable(Float64),
    step Nullable(UInt8),
    step_name Nullable(String)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

-- =====================================================
-- Other tables (unchanged but included for completeness)
-- =====================================================

CREATE TABLE IF NOT EXISTS users (
    user_id String,
    company_name String,
    email String,
    password String,
    is_verify UInt8,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY user_id;

CREATE TABLE IF NOT EXISTS interaction_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    event_type String,
    page_url String,
    x Nullable(UInt16),
    y Nullable(UInt16),
    element String DEFAULT '',
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

CREATE TABLE IF NOT EXISTS mouse_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    page_url String,
    x UInt16,
    y UInt16
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

CREATE TABLE IF NOT EXISTS scroll_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    page_url String,
    event_type String,
    depth_percent Nullable(UInt8),
    scroll_top Nullable(UInt16),
    scroll_percent Nullable(UInt8)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

CREATE TABLE IF NOT EXISTS video_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    page_url String,
    event_type String,
    video_src String,
    video_duration Nullable(Float32),
    current_time Nullable(Float32)
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

CREATE TABLE IF NOT EXISTS custom_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    page_url String,
    event_name LowCardinality(String),
    properties String
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

CREATE TABLE IF NOT EXISTS batch_events (
    timestamp DateTime,
    session_id String,
    user_id String,
    tracking_id String,
    event_data String,
    event_count UInt16
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(timestamp)
ORDER BY (tracking_id, session_id, timestamp);

CREATE TABLE IF NOT EXISTS user_first_session (
    user_id String,
    tracking_id String,
    first_session_time DateTime
) ENGINE = ReplacingMergeTree()
ORDER BY (tracking_id, user_id);

CREATE TABLE IF NOT EXISTS session_pages (
    date Date,
    tracking_id String,
    session_id String,
    entry_page String,
    exit_page String,
    page_count UInt16,
    is_bounce UInt8
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (tracking_id, date, session_id);

CREATE TABLE IF NOT EXISTS aggregation_status (
    table_name String,
    interval_type String,
    tracking_id String,
    time_period DateTime,
    processed_at DateTime DEFAULT now(),
    records_inserted UInt32,
    status String
) ENGINE = MergeTree()
ORDER BY (table_name, interval_type, tracking_id, time_period);

CREATE TABLE IF NOT EXISTS traffic_metrics (
    timestamp DateTime,
    interval_type String,
    tracking_id String,
    unique_users UInt32,
    new_users UInt32,
    returning_users UInt32,
    total_sessions UInt32,
    bounce_sessions UInt32,
    bounce_rate Float32,
    total_pageviews UInt32,
    unique_pageviews UInt32,
    avg_pages_per_session Float32,
    avg_session_duration_sec Float32,
    total_time_on_site_sec UInt64,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY (interval_type, toYYYYMM(timestamp))
ORDER BY (tracking_id, interval_type, timestamp);

CREATE TABLE IF NOT EXISTS page_metrics (
    timestamp DateTime,
    interval_type String,
    tracking_id String,
    page_url String,
    pageviews UInt32,
    unique_visitors UInt32,
    avg_time_on_page_sec Nullable(Float32),
    avg_scroll_depth Nullable(Float32),
    total_clicks UInt32,
    avg_load_time_ms Nullable(Float32),
    p50_load_time_ms Nullable(Float32),
    p95_load_time_ms Nullable(Float32),
    entries UInt32,
    exits UInt32,
    bounces UInt32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY (interval_type, toYYYYMM(timestamp))
ORDER BY (tracking_id, interval_type, timestamp, page_url);

CREATE TABLE IF NOT EXISTS device_metrics (
    timestamp DateTime,
    interval_type String,
    tracking_id String,
    device_type LowCardinality(String),
    operating_system LowCardinality(String),
    browser LowCardinality(String),
    sessions UInt32,
    unique_users UInt32,
    pageviews UInt32,
    avg_session_duration_sec Float32,
    bounce_rate Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY (interval_type, toYYYYMM(timestamp))
ORDER BY (tracking_id, interval_type, timestamp, device_type);

CREATE TABLE IF NOT EXISTS geo_metrics (
    timestamp DateTime,
    interval_type String,
    tracking_id String,
    country LowCardinality(String),
    country_code LowCardinality(String),
    sessions UInt32,
    unique_users UInt32,
    pageviews UInt32,
    avg_session_duration_sec Float32,
    bounce_rate Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY (interval_type, toYYYYMM(timestamp))
ORDER BY (tracking_id, interval_type, timestamp, country_code);

CREATE TABLE IF NOT EXISTS source_metrics (
    timestamp DateTime,
    interval_type String,
    tracking_id String,
    source String,
    referrer String,
    sessions UInt32,
    unique_users UInt32,
    pageviews UInt32,
    avg_session_duration_sec Float32,
    bounce_rate Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY (interval_type, toYYYYMM(timestamp))
ORDER BY (tracking_id, interval_type, timestamp, source);

CREATE TABLE IF NOT EXISTS interaction_metrics (
    timestamp DateTime,
    interval_type String,
    tracking_id String,
    page_url String,
    element String,
    element_id Nullable(String),
    event_type String,
    total_interactions UInt32,
    unique_users UInt32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY (interval_type, toYYYYMM(timestamp))
ORDER BY (tracking_id, interval_type, timestamp, page_url);

CREATE TABLE IF NOT EXISTS form_metrics (
    timestamp DateTime,
    interval_type String,
    tracking_id String,
    form_id String,
    form_name String,
    starts UInt32,
    submissions UInt32,
    successful_submissions UInt32,
    completion_rate Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY (interval_type, toYYYYMM(timestamp))
ORDER BY (tracking_id, interval_type, timestamp, form_id);

CREATE TABLE IF NOT EXISTS ecommerce_metrics (
    timestamp DateTime,
    interval_type String,
    tracking_id String,
    product_views UInt32,
    cart_adds UInt32,
    cart_removes UInt32,
    checkouts UInt32,
    total_orders UInt32,
    total_revenue Float64,
    avg_order_value Float32,
    unique_customers UInt32,
    view_to_cart_rate Float32,
    cart_to_checkout_rate Float32,
    checkout_to_purchase_rate Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY (interval_type, toYYYYMM(timestamp))
ORDER BY (tracking_id, interval_type, timestamp);

CREATE TABLE IF NOT EXISTS product_metrics (
    date Date,
    tracking_id String,
    product_id String,
    product_name String,
    category LowCardinality(String),
    views UInt32,
    cart_adds UInt32,
    purchases UInt32,
    quantity_sold Nullable(UInt64), 
    revenue Nullable(Float64),  
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (tracking_id, date, product_id);

CREATE TABLE IF NOT EXISTS video_metrics (
    date Date,
    tracking_id String,
    video_src String,
    plays UInt32,
    completions UInt32,
    completion_rate Float32,
    watched_25_percent UInt32,
    watched_50_percent UInt32,
    watched_75_percent UInt32,
    unique_viewers UInt32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (tracking_id, date, video_src);

CREATE TABLE IF NOT EXISTS conversion_funnel (
    date Date,
    tracking_id String,
    funnel_step String,
    users UInt32,
    conversion_rate Float32,
    created_at DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(date)
ORDER BY (tracking_id, date, funnel_step);

-- =====================================================
-- BLOOM FILTER INDEXES
-- =====================================================

ALTER TABLE page_events ADD INDEX IF NOT EXISTS idx_tracking_id tracking_id TYPE bloom_filter GRANULARITY 1;
ALTER TABLE page_events ADD INDEX IF NOT EXISTS idx_session_id session_id TYPE bloom_filter GRANULARITY 1;
ALTER TABLE interaction_events ADD INDEX IF NOT EXISTS idx_tracking_id tracking_id TYPE bloom_filter GRANULARITY 1;
ALTER TABLE ecommerce_events ADD INDEX IF NOT EXISTS idx_product_id product_id TYPE bloom_filter GRANULARITY 1;

-- =====================================================
-- ALL 22 MATERIALIZED VIEWS
-- =====================================================

-- MV 1: User First Session
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_user_first_session TO user_first_session AS
SELECT user_id, tracking_id, min(start_time) as first_session_time
FROM sessions GROUP BY user_id, tracking_id;

-- MV 2-4: Traffic Metrics (5m, 1h, 1d)
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_traffic_5m TO traffic_metrics AS
WITH session_agg AS (
    SELECT toStartOfFiveMinutes(start_time) as ts, tracking_id,
        count(*) as session_count, uniq(user_id) as unique_user_count,
        countIf(bounce = 1) as bounce_count, sum(page_views) as total_pv,
        sum(duration_ms) as total_duration_ms
    FROM sessions GROUP BY ts, tracking_id
)
SELECT ts as timestamp, '5m' as interval_type, tracking_id,
    unique_user_count as unique_users, 0 as new_users, 0 as returning_users,
    session_count as total_sessions, bounce_count as bounce_sessions,
    if(session_count > 0, bounce_count * 100.0 / session_count, 0) as bounce_rate,
    total_pv as total_pageviews, 0 as unique_pageviews,
    if(session_count > 0, total_pv / session_count, 0) as avg_pages_per_session,
    if(session_count > 0, total_duration_ms / 1000.0 / session_count, 0) as avg_session_duration_sec,
    total_duration_ms / 1000 as total_time_on_site_sec, now() as created_at
FROM session_agg;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_traffic_1h TO traffic_metrics AS
WITH session_agg AS (
    SELECT toStartOfHour(start_time) as ts, tracking_id,
        count(*) as session_count, uniq(user_id) as unique_user_count,
        countIf(bounce = 1) as bounce_count, sum(page_views) as total_pv,
        sum(duration_ms) as total_duration_ms
    FROM sessions GROUP BY ts, tracking_id
)
SELECT ts as timestamp, '1h' as interval_type, tracking_id,
    unique_user_count as unique_users, 0 as new_users, 0 as returning_users,
    session_count as total_sessions, bounce_count as bounce_sessions,
    if(session_count > 0, bounce_count * 100.0 / session_count, 0) as bounce_rate,
    total_pv as total_pageviews, 0 as unique_pageviews,
    if(session_count > 0, total_pv / session_count, 0) as avg_pages_per_session,
    if(session_count > 0, total_duration_ms / 1000.0 / session_count, 0) as avg_session_duration_sec,
    total_duration_ms / 1000 as total_time_on_site_sec, now() as created_at
FROM session_agg;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_traffic_1d TO traffic_metrics AS
WITH session_agg AS (
    SELECT toStartOfDay(start_time) as ts, tracking_id,
        count(*) as session_count, uniq(user_id) as unique_user_count,
        countIf(bounce = 1) as bounce_count, sum(page_views) as total_pv,
        sum(duration_ms) as total_duration_ms
    FROM sessions GROUP BY ts, tracking_id
)
SELECT ts as timestamp, '1d' as interval_type, tracking_id,
    unique_user_count as unique_users, 0 as new_users, 0 as returning_users,
    session_count as total_sessions, bounce_count as bounce_sessions,
    if(session_count > 0, bounce_count * 100.0 / session_count, 0) as bounce_rate,
    total_pv as total_pageviews, 0 as unique_pageviews,
    if(session_count > 0, total_pv / session_count, 0) as avg_pages_per_session,
    if(session_count > 0, total_duration_ms / 1000.0 / session_count, 0) as avg_session_duration_sec,
    total_duration_ms / 1000 as total_time_on_site_sec, now() as created_at
FROM session_agg;

-- MV 5-6: Page Metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_page_1h TO page_metrics AS
SELECT toStartOfHour(timestamp) as timestamp, '1h' as interval_type, tracking_id, page_url,
    count(*) as pageviews, uniq(session_id) as unique_visitors,
    avgIf(duration_ms, duration_ms IS NOT NULL) / 1000 as avg_time_on_page_sec,
    avgIf(scroll_depth_max, scroll_depth_max IS NOT NULL) as avg_scroll_depth,
    sum(coalesce(click_count, 0)) as total_clicks,
    avgIf(page_load_time, page_load_time IS NOT NULL) as avg_load_time_ms,
    quantileIf(0.5)(page_load_time, page_load_time IS NOT NULL) as p50_load_time_ms,
    quantileIf(0.95)(page_load_time, page_load_time IS NOT NULL) as p95_load_time_ms,
    0 as entries, 0 as exits, 0 as bounces, now() as created_at
FROM page_events GROUP BY timestamp, interval_type, tracking_id, page_url;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_page_1d TO page_metrics AS
SELECT toStartOfDay(timestamp) as timestamp, '1d' as interval_type, tracking_id, page_url,
    count(*) as pageviews, uniq(session_id) as unique_visitors,
    avgIf(duration_ms, duration_ms IS NOT NULL) / 1000 as avg_time_on_page_sec,
    avgIf(scroll_depth_max, scroll_depth_max IS NOT NULL) as avg_scroll_depth,
    sum(coalesce(click_count, 0)) as total_clicks,
    avgIf(page_load_time, page_load_time IS NOT NULL) as avg_load_time_ms,
    quantileIf(0.5)(page_load_time, page_load_time IS NOT NULL) as p50_load_time_ms,
    quantileIf(0.95)(page_load_time, page_load_time IS NOT NULL) as p95_load_time_ms,
    0 as entries, 0 as exits, 0 as bounces, now() as created_at
FROM page_events GROUP BY timestamp, interval_type, tracking_id, page_url;

-- MV 7-8: Device Metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_device_1h TO device_metrics AS
SELECT toStartOfHour(start_time) as timestamp, '1h' as interval_type, tracking_id,
    device_type, operating_system, browser, count(*) as sessions, uniq(user_id) as unique_users,
    sum(page_views) as pageviews, avgIf(duration_ms, duration_ms IS NOT NULL) / 1000 as avg_session_duration_sec,
    if(count(*) > 0, countIf(bounce = 1) * 100.0 / count(*), 0) as bounce_rate, now() as created_at
FROM sessions GROUP BY tracking_id, device_type, operating_system, browser, timestamp;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_device_1d TO device_metrics AS
SELECT toStartOfDay(start_time) as timestamp, '1d' as interval_type, tracking_id,
    device_type, operating_system, browser, count(*) as sessions, uniq(user_id) as unique_users,
    sum(page_views) as pageviews, avgIf(duration_ms, duration_ms IS NOT NULL) / 1000 as avg_session_duration_sec,
    if(count(*) > 0, countIf(bounce = 1) * 100.0 / count(*), 0) as bounce_rate, now() as created_at
FROM sessions GROUP BY tracking_id, device_type, operating_system, browser, timestamp;

-- MV 9-10: Geo Metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_geo_1h TO geo_metrics AS
SELECT toStartOfHour(start_time) as timestamp, '1h' as interval_type, tracking_id,
    coalesce(country, 'Unknown') as country, coalesce(country_code, 'XX') as country_code,
    count(*) as sessions, uniq(user_id) as unique_users,
    sum(page_views) as pageviews, avgIf(duration_ms, duration_ms IS NOT NULL) / 1000 as avg_session_duration_sec,
    if(count(*) > 0, countIf(bounce = 1) * 100.0 / count(*), 0) as bounce_rate, now() as created_at
FROM sessions GROUP BY tracking_id, country, country_code, timestamp;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_geo_1d TO geo_metrics AS
SELECT toStartOfDay(start_time) as timestamp, '1d' as interval_type, tracking_id,
    coalesce(country, 'Unknown') as country, coalesce(country_code, 'XX') as country_code,
    count(*) as sessions, uniq(user_id) as unique_users,
    sum(page_views) as pageviews, avgIf(duration_ms, duration_ms IS NOT NULL) / 1000 as avg_session_duration_sec,
    if(count(*) > 0, countIf(bounce = 1) * 100.0 / count(*), 0) as bounce_rate, now() as created_at
FROM sessions GROUP BY tracking_id, country, country_code, timestamp;

-- MV 11-12: Source Metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_source_1h TO source_metrics AS
SELECT toStartOfHour(start_time) as timestamp, '1h' as interval_type, tracking_id,
    referrer as source, referrer, count(*) as sessions, uniq(user_id) as unique_users,
    sum(page_views) as pageviews, avgIf(duration_ms, duration_ms IS NOT NULL) / 1000 as avg_session_duration_sec,
    if(count(*) > 0, countIf(bounce = 1) * 100.0 / count(*), 0) as bounce_rate, now() as created_at
FROM sessions GROUP BY tracking_id, source, referrer, timestamp;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_source_1d TO source_metrics AS
SELECT toStartOfDay(start_time) as timestamp, '1d' as interval_type, tracking_id,
    referrer as source, referrer, count(*) as sessions, uniq(user_id) as unique_users,
    sum(page_views) as pageviews, avgIf(duration_ms, duration_ms IS NOT NULL) / 1000 as avg_session_duration_sec,
    if(count(*) > 0, countIf(bounce = 1) * 100.0 / count(*), 0) as bounce_rate, now() as created_at
FROM sessions GROUP BY tracking_id, source, referrer, timestamp;

-- MV 13-14: Interaction Metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_interaction TO interaction_metrics AS
SELECT toStartOfHour(timestamp) as timestamp, '1h' as interval_type, tracking_id,
    page_url, element, element_id, event_type,
    count(*) as total_interactions, uniq(user_id) as unique_users, now() as created_at
FROM interaction_events GROUP BY tracking_id, page_url, element, element_id, event_type, timestamp;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_interaction_1d TO interaction_metrics AS
SELECT toStartOfDay(timestamp) as timestamp, '1d' as interval_type, tracking_id,
    page_url, element, element_id, event_type,
    count(*) as total_interactions, uniq(user_id) as unique_users, now() as created_at
FROM interaction_events GROUP BY timestamp, interval_type, tracking_id, page_url, element, element_id, event_type;

-- MV 15-16: Form Metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_form TO form_metrics AS
SELECT toStartOfHour(timestamp) as timestamp, '1h' as interval_type, tracking_id,
    form_id, coalesce(form_name, '') as form_name,
    countIf(event_type = 'form_focus') as starts,
    countIf(event_type = 'form_submit') as submissions,
    countIf(success = 1 AND event_type = 'form_submit') as successful_submissions,
    if(countIf(event_type = 'form_focus') > 0,
       countIf(success = 1 AND event_type = 'form_submit') * 100.0 / countIf(event_type = 'form_focus'), 0) as completion_rate,
    now() as created_at
FROM form_events GROUP BY tracking_id, form_id, form_name, timestamp;

CREATE MATERIALIZED VIEW IF NOT EXISTS mv_form_1d TO form_metrics AS
SELECT toStartOfDay(timestamp) as timestamp, '1d' as interval_type, tracking_id,
    form_id, any(form_name) as form_name,
    countIf(event_type = 'form_focus') as starts,
    countIf(event_type = 'form_submit') as submissions,
    countIf(event_type = 'form_submit' AND success = 1) as successful_submissions,
    if(countIf(event_type = 'form_focus') > 0,
       countIf(event_type = 'form_submit') * 100.0 / countIf(event_type = 'form_focus'), 0) as completion_rate,
    now() as created_at
FROM form_events GROUP BY timestamp, interval_type, tracking_id, form_id;

-- MV 17-18: E-commerce 

CREATE MATERIALIZED VIEW mv_ecommerce TO ecommerce_metrics AS
SELECT 
    toStartOfHour(timestamp) as timestamp, 
    '1h' as interval_type, 
    tracking_id,
    CAST(countIf(event_type = 'product_view') AS UInt32) as product_views,
    CAST(countIf(event_type = 'cart_add') AS UInt32) as cart_adds,
    CAST(countIf(event_type = 'cart_remove') AS UInt32) as cart_removes,
    CAST(countIf(event_type = 'checkout_step') AS UInt32) as checkouts,
    CAST(countIf(event_type = 'purchase') AS UInt32) as total_orders,
    sumIf(total, event_type = 'purchase') as total_revenue,
    CAST(if(countIf(event_type = 'purchase') > 0, sumIf(total, event_type = 'purchase') / countIf(event_type = 'purchase'), 0) AS Float32) as avg_order_value,
    CAST(uniqIf(user_id, event_type = 'purchase') AS UInt32) as unique_customers,
    CAST(if(countIf(event_type = 'product_view') > 0, countIf(event_type = 'cart_add') * 100.0 / countIf(event_type = 'product_view'), 0) AS Float32) as view_to_cart_rate,
    CAST(if(countIf(event_type = 'cart_add') > 0, countIf(event_type = 'checkout_step') * 100.0 / countIf(event_type = 'cart_add'), 0) AS Float32) as cart_to_checkout_rate,
    CAST(if(countIf(event_type = 'checkout_step') > 0, countIf(event_type = 'purchase') * 100.0 / countIf(event_type = 'checkout_step'), 0) AS Float32) as checkout_to_purchase_rate,
    now() as created_at
FROM ecommerce_events 
GROUP BY tracking_id, toStartOfHour(timestamp);

CREATE MATERIALIZED VIEW mv_ecommerce_1d TO ecommerce_metrics AS
SELECT 
    day_timestamp as timestamp, 
    '1d' as interval_type, 
    tracking_id,
    CAST(countIf(event_type = 'product_view') AS UInt32) as product_views,
    CAST(countIf(event_type = 'cart_add') AS UInt32) as cart_adds,
    CAST(countIf(event_type = 'cart_remove') AS UInt32) as cart_removes,
    CAST(countIf(event_type = 'checkout_step') AS UInt32) as checkouts,
    CAST(countIf(event_type = 'purchase') AS UInt32) as total_orders,
    CAST(sumIf(total, event_type = 'purchase') AS Float64) as total_revenue,
    CAST(avgIf(total, event_type = 'purchase') AS Float32) as avg_order_value,
    CAST(uniqIf(user_id, event_type = 'purchase') AS UInt32) as unique_customers,
    CAST(if(countIf(event_type = 'product_view') > 0, countIf(event_type = 'cart_add') * 100.0 / countIf(event_type = 'product_view'), 0) AS Float32) as view_to_cart_rate,
    CAST(if(countIf(event_type = 'cart_add') > 0, countIf(event_type = 'checkout_step') * 100.0 / countIf(event_type = 'cart_add'), 0) AS Float32) as cart_to_checkout_rate,
    CAST(if(countIf(event_type = 'checkout_step') > 0, countIf(event_type = 'purchase') * 100.0 / countIf(event_type = 'checkout_step'), 0) AS Float32) as checkout_to_purchase_rate,
    now() as created_at
FROM (
    SELECT 
        toStartOfDay(timestamp) as day_timestamp,
        tracking_id,
        event_type,
        total,
        user_id
    FROM ecommerce_events
) 
GROUP BY day_timestamp, tracking_id;

-- MV 19: Product Metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_product TO product_metrics AS
SELECT 
    toDate(timestamp) as date,
    tracking_id,
    product_id,
    any(product_name) as product_name,
    any(category) as category,
    countIf(event_type = 'product_view') as views,
    countIf(event_type = 'cart_add') as cart_adds,
    countIf(event_type = 'purchase') as purchases,
    -- ✅ SIMPLEST FIX: Convert NULL to 0
    CAST(coalesce(sumIf(quantity, event_type = 'purchase'), 0) AS Nullable(UInt64)) as quantity_sold,
    CAST(coalesce(sumIf(total, event_type = 'purchase'), 0.0) AS Nullable(Float64)) as revenue,
    now() as created_at
FROM ecommerce_events
WHERE product_id IS NOT NULL
GROUP BY tracking_id, date, product_id;

-- MV 20: Video Metrics
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_video TO video_metrics AS
SELECT toDate(timestamp) as date, tracking_id, video_src,
    countIf(event_type = 'play') as plays,
    countIf(event_type = 'complete') as completions,
    if(countIf(event_type = 'play') > 0, countIf(event_type = 'complete') * 100.0 / countIf(event_type = 'play'), 0) as completion_rate,
    countIf(event_type = 'progress_25') as watched_25_percent,
    countIf(event_type = 'progress_50') as watched_50_percent,
    countIf(event_type = 'progress_75') as watched_75_percent,
    uniq(user_id) as unique_viewers,
    now() as created_at
FROM video_events GROUP BY tracking_id, date, video_src;

-- MV 21: Session Pages
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_session_pages TO session_pages AS
WITH page_sequence AS (
    SELECT toDate(timestamp) as date, tracking_id, session_id,
        argMin(page_url, timestamp) as entry_page,
        argMax(page_url, timestamp) as exit_page,
        count(*) as page_count
    FROM page_events
    WHERE event_type = 'page_view'
    GROUP BY date, tracking_id, session_id
)
SELECT date, tracking_id, session_id, entry_page, exit_page, page_count,
    if(page_count = 1, 1, 0) as is_bounce
FROM page_sequence;

-- MV 22: Conversion Funnel
CREATE MATERIALIZED VIEW IF NOT EXISTS mv_conversion_funnel TO conversion_funnel AS
SELECT toDate(timestamp) as date, tracking_id,
    'page_view' as funnel_step,
    uniq(user_id) as users,
    0 as conversion_rate,
    now() as created_at
FROM page_events
WHERE event_type = 'page_view'
GROUP BY date, tracking_id
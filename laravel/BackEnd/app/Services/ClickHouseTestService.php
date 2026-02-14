<?php

namespace App\Services;

use ClickHouseDB\Client;

class ClickHouseTestService
{
    protected Client $client;

    public function __construct()
    {
        $this->client = new Client([
            "host"     => env("CLICKHOUSE_HOST", "127.0.0.1"),
            "port"     => env("CLICKHOUSE_PORT", 8123),
            "username" => env("CLICKHOUSE_USERNAME", "default"),
            "password" => env("CLICKHOUSE_PASSWORD", "root"),
        ]);

        $this->client->database(env("CLICKHOUSE_DATABASE", "default"));
    }

 
    public function insertEvent(array $data)
    {
        $this->client->insert(
            "page_events",
            [[
                now()->toDateTimeString(),
                $data["session_id"],
                $data["user_id"],
                $data["tracking_id"],
                $data["event_type"],
                $data["page_url"],
                $data["page_title"] ?? '',
                $data["referrer"] ?? '',
                null, null, null, null, null, null, null, null, null, null, null, null
            ]],
            [
                'timestamp', 'session_id', 'user_id', 'tracking_id', 'event_type',
                'page_url', 'page_title', 'referrer', 'duration_ms', 'scroll_depth_max',
                'click_count', 'dns_time', 'connect_time', 'response_time', 'dom_load_time',
                'page_load_time', 'connection_type', 'connection_downlink', 'connection_rtt', 'save_data'
            ]
        );
    }

    public function countEventsForUser(string $userId): int
    {
        $result = $this->client->select(
            "SELECT count() AS cnt FROM page_events WHERE user_id = :user_id",
            ['user_id' => $userId]
        );
        $rows = $result->rows();
        return isset($rows[0]['cnt']) ? (int) $rows[0]['cnt'] : 0;
    }

    public function getLastEventsForUser(string $userId, int $limit = 20): array
    {
        $result = $this->client->select(
            "SELECT timestamp, session_id, user_id, tracking_id, event_type, page_url, page_title, referrer
             FROM page_events
             WHERE user_id = :user_id
             ORDER BY timestamp DESC
             LIMIT :limit",
            ['user_id' => $userId, 'limit' => $limit]
        );
        return $result->rows() ?? [];
    }

   
    public function getOnlineUsersCount(string $trackingId = null): int
    {
        $where = $trackingId
            ? "WHERE tracking_id = :tracking_id AND (end_time IS NULL OR end_time > now() - INTERVAL 5 MINUTE)"
            : "WHERE end_time IS NULL OR end_time > now() - INTERVAL 5 MINUTE";
        $params = $trackingId ? ['tracking_id' => $trackingId] : [];

        $result = $this->client->select(
            "SELECT count(DISTINCT user_id) AS cnt FROM sessions $where",
            $params
        );
        $rows = $result->rows();
        return $rows[0]['cnt'] ?? 0;
    }

    public function getLivePageViews(int $seconds = 5, string $trackingId = null): int
    {
        $where = $trackingId
            ? "WHERE tracking_id = :tracking_id AND timestamp >= now() - INTERVAL :seconds SECOND"
            : "WHERE timestamp >= now() - INTERVAL :seconds SECOND";
        $params = $trackingId ? ['tracking_id' => $trackingId, 'seconds' => $seconds] : ['seconds' => $seconds];

        $result = $this->client->select(
            "SELECT count() AS cnt FROM page_events $where",
            $params
        );
        $rows = $result->rows();
        return $rows[0]['cnt'] ?? 0;
    }

    public function getLiveInteractions(string $trackingId = null): array
    {
        $where = $trackingId ? "WHERE tracking_id = :tracking_id AND timestamp >= now() - INTERVAL 5 MINUTE"
            : "WHERE timestamp >= now() - INTERVAL 5 MINUTE";
        $params = $trackingId ? ['tracking_id' => $trackingId] : [];

        $result = $this->client->select(
            "SELECT event_type, count() as count, uniq(user_id) as unique_users
             FROM interaction_events
             $where
             GROUP BY event_type
             ORDER BY count DESC
             LIMIT 10",
            $params
        );
        return $result->rows() ?? [];
    }

    public function getLiveFormSubmissions(string $trackingId = null): array
    {
        $where = $trackingId ? "WHERE tracking_id = :tracking_id AND timestamp >= now() - INTERVAL 5 MINUTE"
            : "WHERE timestamp >= now() - INTERVAL 5 MINUTE";
        $params = $trackingId ? ['tracking_id' => $trackingId] : [];

        $result = $this->client->select(
            "SELECT form_id, any(form_name) as form_name,
                    countIf(event_type = 'form_submit') as submissions,
                    countIf(event_type = 'form_submit' AND success = 1) as successful
             FROM form_events
             $where
             GROUP BY form_id
             ORDER BY submissions DESC
             LIMIT 10",
            $params
        );
        return $result->rows() ?? [];
    }

    public function getLiveUserLocations(string $trackingId = null): array
    {
        $where = $trackingId
            ? "WHERE tracking_id = :tracking_id AND start_time >= now() - INTERVAL 5 MINUTE"
            : "WHERE start_time >= now() - INTERVAL 5 MINUTE";
        $params = $trackingId ? ['tracking_id' => $trackingId] : [];

        $result = $this->client->select(
            "SELECT coalesce(country, 'Unknown') as country,
                    coalesce(country_code, 'XX') as country_code,
                    uniq(user_id) as users
             FROM sessions
             $where
             GROUP BY country, country_code
             ORDER BY users DESC
             LIMIT 20",
            $params
        );

        return $result->rows() ?? [];
    }

    public function getLivePopularPages(string $trackingId = null): array
    {
        $where = $trackingId ? "WHERE tracking_id = :tracking_id AND timestamp >= now() - INTERVAL 5 MINUTE"
            : "WHERE timestamp >= now() - INTERVAL 5 MINUTE";
        $params = $trackingId ? ['tracking_id' => $trackingId] : [];

        $result = $this->client->select(
            "SELECT page_url, count() as views, uniq(session_id) as unique_visitors,
                    avgIf(duration_ms, duration_ms IS NOT NULL) / 1000 as avg_time_sec
             FROM page_events
             $where
             GROUP BY page_url
             ORDER BY views DESC
             LIMIT 10",
            $params
        );

        return $result->rows() ?? [];
    }

  
    public function getPageAnalytics(string $trackingId, string $interval = '1h'): array
    {
        $result = $this->client->select(
            "SELECT page_url,
                    sum(pageviews) as pageviews,
                    sum(unique_visitors) as unique_visitors,
                    avg(avg_time_on_page_sec) as avg_time_on_page_sec,
                    avg(avg_scroll_depth) as avg_scroll_depth,
                    sum(total_clicks) as total_clicks,
                    avg(avg_load_time_ms) as avg_load_time_ms
             FROM page_metrics
             WHERE tracking_id = :tracking_id AND interval_type = :interval
             GROUP BY page_url
             ORDER BY pageviews DESC
             LIMIT 50",
            ['tracking_id' => $trackingId, 'interval' => $interval]
        );
        return $result->rows() ?? [];
    }

    public function getSessionsForUser(string $userId, int $limit = 50): array
    {
        $result = $this->client->select(
            "SELECT session_id, user_id, tracking_id, start_time, end_time,
                    coalesce(duration_ms, 0) / 1000 as duration_sec, 
                    page_views, bounce,
                    entry_page, exit_page, device_type, operating_system, browser,
                    country, country_code
             FROM sessions
             WHERE user_id = :user_id
             ORDER BY start_time DESC
             LIMIT :limit",
            ['user_id' => $userId, 'limit' => $limit]
        );
        return $result->rows() ?? [];
    }

    public function getBounceRateByHour(string $trackingId): array
    {
        $result = $this->client->select(
            "SELECT toStartOfHour(timestamp) as hour,
                    sum(total_sessions) as total_sessions,
                    sum(bounce_sessions) as bounce_sessions,
                    avg(bounce_rate) as bounce_rate
             FROM traffic_metrics
             WHERE tracking_id = :tracking_id AND interval_type = '1h'
                   AND timestamp >= now() - INTERVAL 24 HOUR
             GROUP BY hour
             ORDER BY hour DESC",
            ['tracking_id' => $trackingId]
        );
        return $result->rows() ?? [];
    }

    public function getDeviceBreakdown(string $trackingId): array
    {
        $result = $this->client->select(
            "SELECT device_type, operating_system, browser,
                    sum(sessions) as sessions,
                    sum(unique_users) as unique_users,
                    sum(pageviews) as pageviews,
                    avg(avg_session_duration_sec) as avg_session_duration_sec,
                    avg(bounce_rate) as bounce_rate
             FROM device_metrics
             WHERE tracking_id = :tracking_id AND interval_type = '1d'
                   AND timestamp >= today() - INTERVAL 7 DAY
             GROUP BY device_type, operating_system, browser
             ORDER BY sessions DESC
             LIMIT 50",
            ['tracking_id' => $trackingId]
        );
        return $result->rows() ?? [];
    }

    public function getScreenResolutions(string $trackingId): array
    {
        $result = $this->client->select(
            "SELECT concat(toString(screen_width), 'x', toString(screen_height)) as resolution,
                    count() as sessions,
                    uniq(user_id) as unique_users
             FROM sessions
             WHERE tracking_id = :tracking_id AND start_time >= today() - INTERVAL 7 DAY
             GROUP BY resolution
             ORDER BY sessions DESC
             LIMIT 20",
            ['tracking_id' => $trackingId]
        );
        return $result->rows() ?? [];
    }

    public function getMobileVsDesktop(string $trackingId): array
    {
        $result = $this->client->select(
            "SELECT device_type,
                    sum(sessions) as sessions,
                    sum(unique_users) as unique_users,
                    sum(pageviews) as pageviews
             FROM device_metrics
             WHERE tracking_id = :tracking_id AND interval_type = '1d'
                   AND timestamp >= today() - INTERVAL 7 DAY
             GROUP BY device_type
             ORDER BY sessions DESC",
            ['tracking_id' => $trackingId]
        );

        $rows = $result->rows() ?? [];
        $total = array_sum(array_column($rows, 'sessions'));

        foreach ($rows as &$row) {
            $row['percentage'] = $total > 0 ? round(($row['sessions'] * 100.0) / $total, 2) : 0;
        }

        return $rows;
    }

    public function getGeographicData(string $trackingId): array
    {
        $result = $this->client->select(
            "SELECT country, country_code,
                    sum(sessions) as sessions,
                    sum(unique_users) as unique_users,
                    sum(pageviews) as pageviews,
                    avg(avg_session_duration_sec) as avg_session_duration_sec
             FROM geo_metrics
             WHERE tracking_id = :tracking_id AND interval_type = '1d'
                   AND timestamp >= today() - INTERVAL 7 DAY
             GROUP BY country, country_code
             ORDER BY sessions DESC
             LIMIT 100",
            ['tracking_id' => $trackingId]
        );
        return $result->rows() ?? [];
    }

    public function getConnectionQuality(string $trackingId): array
    {
        $result = $this->client->select(
            "SELECT coalesce(connection_type, 'Unknown') as connection_type,
                    count() as events,
                    avgIf(connection_downlink, connection_downlink IS NOT NULL) as avg_downlink_mbps,
                    avgIf(connection_rtt, connection_rtt IS NOT NULL) as avg_rtt_ms,
                    sumIf(save_data, save_data = 1) as save_data_enabled
             FROM page_events
             WHERE tracking_id = :tracking_id AND timestamp >= today() - INTERVAL 7 DAY
                   AND connection_type IS NOT NULL
             GROUP BY connection_type
             ORDER BY events DESC",
            ['tracking_id' => $trackingId]
        );
        return $result->rows() ?? [];
    }

   
    public function getScrollDepthAnalysis(string $trackingId): array
    {
        $result = $this->client->select(
            "SELECT page_url,
                    avgIf(scroll_depth_max, scroll_depth_max IS NOT NULL) as avg_scroll_depth,
                    quantileIf(0.5)(scroll_depth_max, scroll_depth_max IS NOT NULL) as median_scroll_depth,
                    quantileIf(0.9)(scroll_depth_max, scroll_depth_max IS NOT NULL) as p90_scroll_depth,
                    count() as sessions
             FROM page_events
             WHERE tracking_id = :tracking_id AND timestamp >= today() - INTERVAL 7 DAY
                   AND scroll_depth_max IS NOT NULL
             GROUP BY page_url
             ORDER BY sessions DESC
             LIMIT 50",
            ['tracking_id' => $trackingId]
        );
        return $result->rows() ?? [];
    }

    public function getFormPatterns(string $trackingId): array
    {
        $result = $this->client->select(
            "SELECT form_id, any(form_name) as form_name,
                    sum(starts) as form_starts,
                    0 as field_interactions,
                    sum(submissions) as submissions,
                    sum(successful_submissions) as successful_submissions,
                    avg(completion_rate) as conversion_rate
             FROM form_metrics
             WHERE tracking_id = :tracking_id AND interval_type = '1d'
                   AND timestamp >= today() - INTERVAL 7 DAY
             GROUP BY form_id
             ORDER BY submissions DESC
             LIMIT 20",
            ['tracking_id' => $trackingId]
        );
        return $result->rows() ?? [];
    }

    public function getDownloadsAndClicks(string $trackingId): array
    {
        $result = $this->client->select(
            "SELECT event_type,
                    coalesce(file_name, link_url, '') as resource,
                    any(link_text) as link_text,
                    any(is_external) as is_external,
                    count() as clicks,
                    uniq(user_id) as unique_users
             FROM interaction_events
             WHERE tracking_id = :tracking_id AND timestamp >= today() - INTERVAL 7 DAY
                   AND event_type IN ('file_download', 'link_click')
             GROUP BY event_type, resource
             ORDER BY clicks DESC
             LIMIT 50",
            ['tracking_id' => $trackingId]
        );
        return $result->rows() ?? [];
    }

    public function getVideoEngagement(string $trackingId): array
    {
        $result = $this->client->select(
            "SELECT video_src,
                    sum(plays) as plays,
                    sum(completions) as completions,
                    avg(completion_rate) as completion_rate,
                    sum(watched_25_percent) as watched_25_percent,
                    sum(watched_50_percent) as watched_50_percent,
                    sum(watched_75_percent) as watched_75_percent,
                    sum(unique_viewers) as unique_viewers,
                    0 as avg_video_duration_sec
             FROM video_metrics
             WHERE tracking_id = :tracking_id AND date >= today() - INTERVAL 7 DAY
             GROUP BY video_src
             ORDER BY plays DESC
             LIMIT 20",
            ['tracking_id' => $trackingId]
        );
        return $result->rows() ?? [];
    }

    
   public function getProductAnalytics(string $trackingId): array
{
    $result = $this->client->select(
        "SELECT *,
                if(views > 0, cart_adds * 100.0 / views, 0) AS view_to_cart_rate,
                if(cart_adds > 0, purchases * 100.0 / cart_adds, 0) AS cart_to_purchase_rate
         FROM (
            SELECT product_id,
                   any(product_name) AS product_name,
                   any(category) AS category,
                   sum(views) AS views,
                   sum(cart_adds) AS cart_adds,
                   sum(purchases) AS purchases,
                   sum(coalesce(quantity_sold, 0)) AS quantity_sold,
                   sum(coalesce(revenue, 0)) AS revenue
            FROM product_metrics
            WHERE tracking_id = :tracking_id AND date >= today() - INTERVAL 7 DAY
            GROUP BY product_id
         )
         ORDER BY revenue DESC
         LIMIT 100",
        ['tracking_id' => $trackingId]
    );

    return $result->rows() ?? [];
}

    public function getCartAnalytics(string $trackingId): array
    {
        $result = $this->client->select(
            "SELECT toStartOfHour(timestamp) as hour,
                    sumIf(1, event_type = 'cart_add') as cart_adds,
                    sumIf(1, event_type = 'cart_remove') as cart_removes,
                    sumIf(1, event_type = 'cart_view') as cart_views,
                    uniq(session_id) as unique_sessions
             FROM ecommerce_events
             WHERE tracking_id = :tracking_id AND timestamp >= now() - INTERVAL 24 HOUR
             GROUP BY hour
             ORDER BY hour DESC",
            ['tracking_id' => $trackingId]
        );
        return $result->rows() ?? [];
    }


public function getUserJourney(string $userId, ?string $sessionId = null): array
{
    try {
        $query = "
            SELECT 
                timestamp,
                session_id,
                page_url,
                page_title,
                referrer,
                duration_ms,
                scroll_depth_max,
                click_count,
                page_load_time,
                ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY timestamp) as step_number
            FROM page_events
            WHERE user_id = :user_id
        ";

        $params = ['user_id' => $userId];

        if ($sessionId) {
            $query .= " AND session_id = :session_id";
            $params['session_id'] = $sessionId;
        }

        $query .= " ORDER BY timestamp ASC LIMIT 1000";

        $result = $this->client->select($query, $params);

        $sessions = [];
        foreach ($result->rows() as $row) {
            $sessionId = $row['session_id'];
            
            if (!isset($sessions[$sessionId])) {
                $sessions[$sessionId] = [
                    'session_id' => $sessionId,
                    'pages' => [],
                    'total_pages' => 0,
                    'total_duration' => 0,
                ];
            }

            $sessions[$sessionId]['pages'][] = [
                'step' => (int) $row['step_number'],
                'timestamp' => $row['timestamp'],
                'page_url' => $row['page_url'],
                'page_title' => $row['page_title'],
                'referrer' => $row['referrer'],
                'duration_ms' => (int) $row['duration_ms'],
                'scroll_depth' => (float) $row['scroll_depth_max'],
                'clicks' => (int) $row['click_count'],
                'load_time_ms' => (int) $row['page_load_time'],
            ];

            $sessions[$sessionId]['total_pages']++;
            $sessions[$sessionId]['total_duration'] += (int) $row['duration_ms'];
        }

        return [
            'user_id' => $userId,
            'total_sessions' => count($sessions),
            'sessions' => array_values($sessions),
        ];

    } catch (\Exception $e) {
        \Log::error('ClickHouse getUserJourney error: ' . $e->getMessage());
        return [
            'user_id' => $userId,
            'total_sessions' => 0,
            'sessions' => [],
            'error' => $e->getMessage(),
        ];
    }
}



public function getTrafficSourcesBreakdown(string $trackingId): array
{
    try {
        $query = "
            SELECT 
                CASE
                    WHEN referrer = 'direct' OR referrer = '' THEN 'direct'
                    WHEN referrer LIKE '%google%' OR referrer LIKE '%bing%' OR referrer LIKE '%yahoo%' THEN 'search'
                    WHEN referrer LIKE '%facebook%' OR referrer LIKE '%twitter%' OR referrer LIKE '%instagram%' OR referrer LIKE '%linkedin%' THEN 'social'
                    ELSE 'referral'
                END as source_type,
                referrer,
                COUNT(DISTINCT session_id) as sessions,
                COUNT(DISTINCT user_id) as unique_users,
                COUNT(*) as pageviews,
                AVG(duration_ms) / 1000 as avg_session_duration_sec,
                ROUND(SUM(bounce) * 100.0 / COUNT(DISTINCT session_id), 2) as bounce_rate
            FROM sessions
            WHERE tracking_id = :tracking_id
            GROUP BY source_type, referrer
            ORDER BY sessions DESC
            LIMIT 50
        ";

        $result = $this->client->select($query, ['tracking_id' => $trackingId]);

        $sources = [];
        foreach ($result->rows() as $row) {
            $sources[] = [
                'source_type' => $row['source_type'],
                'referrer' => $row['referrer'],
                'sessions' => (int) $row['sessions'],
                'unique_users' => (int) $row['unique_users'],
                'pageviews' => (int) $row['pageviews'],
                'avg_session_duration_sec' => round((float) $row['avg_session_duration_sec'], 2),
                'bounce_rate' => (float) $row['bounce_rate'],
            ];
        }

        return $sources;

    } catch (\Exception $e) {
        \Log::error('ClickHouse getTrafficSourcesBreakdown error: ' . $e->getMessage());
        return [];
    }
}




public function getEntryExitPages(string $trackingId): array
{
    try {
        $entryQuery = "
            SELECT 
                'entry' as page_type,
                entry_page as page_url,
                '' as page_title,
                COUNT(*) as sessions,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sessions WHERE tracking_id = :tracking_id), 2) as percentage
            FROM sessions
            WHERE tracking_id = :tracking_id
            AND entry_page != ''
            GROUP BY entry_page
            ORDER BY sessions DESC
            LIMIT 10
        ";

        $exitQuery = "
            SELECT 
                'exit' as page_type,
                exit_page as page_url,
                '' as page_title,
                COUNT(*) as sessions,
                ROUND(COUNT(*) * 100.0 / (SELECT COUNT(*) FROM sessions WHERE tracking_id = :tracking_id AND exit_page != ''), 2) as percentage
            FROM sessions
            WHERE tracking_id = :tracking_id
            AND exit_page != ''
            GROUP BY exit_page
            ORDER BY sessions DESC
            LIMIT 10
        ";

        $entryResult = $this->client->select($entryQuery, ['tracking_id' => $trackingId]);
        $exitResult = $this->client->select($exitQuery, ['tracking_id' => $trackingId]);

        $pages = [];

        foreach ($entryResult->rows() as $row) {
            $pages[] = [
                'page_type' => 'entry',
                'page_url' => $row['page_url'],
                'page_title' => $row['page_title'],
                'sessions' => (int) $row['sessions'],
                'percentage' => (float) $row['percentage'],
            ];
        }

        foreach ($exitResult->rows() as $row) {
            $pages[] = [
                'page_type' => 'exit',
                'page_url' => $row['page_url'],
                'page_title' => $row['page_title'],
                'sessions' => (int) $row['sessions'],
                'percentage' => (float) $row['percentage'],
            ];
        }

        return $pages;

    } catch (\Exception $e) {
        \Log::error('ClickHouse getEntryExitPages error: ' . $e->getMessage());
        return [];
    }
}



public function getAverageSessionDuration(string $trackingId): float
{
    try {
        $query = "
            SELECT AVG(duration_ms) / 1000 as avg_duration_sec
            FROM sessions
            WHERE tracking_id = :tracking_id
            AND duration_ms > 0
        ";

        $result = $this->client->select($query, ['tracking_id' => $trackingId]);
        
        return round((float) ($result->rows()[0]['avg_duration_sec'] ?? 0), 2);

    } catch (\Exception $e) {
        \Log::error('ClickHouse getAverageSessionDuration error: ' . $e->getMessage());
        return 0.0;
    }
}


public function getPageLoadPerformance(string $trackingId): array
{
    try {
        $query = "
            SELECT 
                page_url,
                AVG(page_load_time) as avg_load_time_ms,
                quantile(0.5)(page_load_time) as median_load_time_ms,
                quantile(0.9)(page_load_time) as p90_load_time_ms,
                quantile(0.95)(page_load_time) as p95_load_time_ms,
                COUNT(*) as samples
            FROM page_events
            WHERE tracking_id = :tracking_id
            AND page_load_time > 0
            GROUP BY page_url
            ORDER BY samples DESC
            LIMIT 20
        ";

        $result = $this->client->select($query, ['tracking_id' => $trackingId]);

        $performance = [];
        foreach ($result->rows() as $row) {
            $performance[] = [
                'page_url' => $row['page_url'],
                'avg_load_time_ms' => round((float) $row['avg_load_time_ms'], 2),
                'median_load_time_ms' => round((float) $row['median_load_time_ms'], 2),
                'p90_load_time_ms' => round((float) $row['p90_load_time_ms'], 2),
                'p95_load_time_ms' => round((float) $row['p95_load_time_ms'], 2),
                'samples' => (int) $row['samples'],
            ];
        }

        return $performance;

    } catch (\Exception $e) {
        \Log::error('ClickHouse getPageLoadPerformance error: ' . $e->getMessage());
        return [];
    }
}



public function getBrowserBreakdown(string $trackingId): array
{
    try {
        $query = "
            SELECT 
                browser,
                COUNT(DISTINCT session_id) as sessions,
                COUNT(DISTINCT user_id) as unique_users,
                SUM(page_views) as pageviews,
                ROUND(COUNT(DISTINCT session_id) * 100.0 / (SELECT COUNT(DISTINCT session_id) FROM sessions WHERE tracking_id = :tracking_id), 2) as percentage
            FROM sessions
            WHERE tracking_id = :tracking_id
            AND browser != ''
            GROUP BY browser
            ORDER BY sessions DESC
        ";

        $result = $this->client->select($query, ['tracking_id' => $trackingId]);

        $browsers = [];
        foreach ($result->rows() as $row) {
            $browsers[] = [
                'browser' => $row['browser'],
                'sessions' => (int) $row['sessions'],
                'unique_users' => (int) $row['unique_users'],
                'pageviews' => (int) $row['pageviews'],
                'percentage' => (float) $row['percentage'],
            ];
        }

        return $browsers;

    } catch (\Exception $e) {
        \Log::error('ClickHouse getBrowserBreakdown error: ' . $e->getMessage());
        return [];
    }
}


public function getOperatingSystemBreakdown(string $trackingId): array
{
    try {
        $query = "
            SELECT 
                operating_system,
                COUNT(DISTINCT session_id) as sessions,
                COUNT(DISTINCT user_id) as unique_users,
                SUM(page_views) as pageviews,
                ROUND(COUNT(DISTINCT session_id) * 100.0 / (SELECT COUNT(DISTINCT session_id) FROM sessions WHERE tracking_id = :tracking_id), 2) as percentage
            FROM sessions
            WHERE tracking_id = :tracking_id
            AND operating_system != ''
            GROUP BY operating_system
            ORDER BY sessions DESC
        ";

        $result = $this->client->select($query, ['tracking_id' => $trackingId]);

        $systems = [];
        foreach ($result->rows() as $row) {
            $systems[] = [
                'operating_system' => $row['operating_system'],
                'sessions' => (int) $row['sessions'],
                'unique_users' => (int) $row['unique_users'],
                'pageviews' => (int) $row['pageviews'],
                'percentage' => (float) $row['percentage'],
            ];
        }

        return $systems;

    } catch (\Exception $e) {
        \Log::error('ClickHouse getOperatingSystemBreakdown error: ' . $e->getMessage());
        return [];
    }
}



public function getHourlyTrafficPattern(string $trackingId): array
{
    try {
        $query = "
            SELECT 
                toHour(start_time) as hour,
                COUNT(DISTINCT session_id) as sessions,
                COUNT(DISTINCT user_id) as unique_users,
                SUM(page_views) as pageviews,
                AVG(duration_ms) / 1000 as avg_session_duration_sec
            FROM sessions
            WHERE tracking_id = :tracking_id
            GROUP BY hour
            ORDER BY hour ASC
        ";

        $result = $this->client->select($query, ['tracking_id' => $trackingId]);

        $pattern = [];
        foreach ($result->rows() as $row) {
            $pattern[] = [
                'hour' => (int) $row['hour'],
                'sessions' => (int) $row['sessions'],
                'unique_users' => (int) $row['unique_users'],
                'pageviews' => (int) $row['pageviews'],
                'avg_session_duration_sec' => round((float) $row['avg_session_duration_sec'], 2),
            ];
        }

        return $pattern;

    } catch (\Exception $e) {
        \Log::error('ClickHouse getHourlyTrafficPattern error: ' . $e->getMessage());
        return [];
    }
}

public function getDailyActiveUsers(string $trackingId, int $days = 30): array
{
    try {
        $query = "
            SELECT 
                toDate(start_time) as date,
                COUNT(DISTINCT user_id) as active_users,
                COUNT(DISTINCT CASE WHEN start_time = (
                    SELECT MIN(start_time) 
                    FROM sessions s2 
                    WHERE s2.user_id = sessions.user_id
                ) THEN user_id END) as new_users,
                COUNT(DISTINCT user_id) - COUNT(DISTINCT CASE WHEN start_time = (
                    SELECT MIN(start_time) 
                    FROM sessions s2 
                    WHERE s2.user_id = sessions.user_id
                ) THEN user_id END) as returning_users
            FROM sessions
            WHERE tracking_id = :tracking_id
            AND start_time >= now() - INTERVAL :days DAY
            GROUP BY date
            ORDER BY date DESC
        ";

        $result = $this->client->select($query, [
            'tracking_id' => $trackingId,
            'days' => $days
        ]);

        $dailyUsers = [];
        foreach ($result->rows() as $row) {
            $dailyUsers[] = [
                'date' => $row['date'],
                'active_users' => (int) $row['active_users'],
                'new_users' => (int) $row['new_users'],
                'returning_users' => (int) $row['returning_users'],
            ];
        }

        return $dailyUsers;

    } catch (\Exception $e) {
        \Log::error('ClickHouse getDailyActiveUsers error: ' . $e->getMessage());
        return [];
    }
}



public function getNewVsReturningUsers(string $trackingId): array
{
    try {
        $query = "
            WITH user_first_session AS (
                SELECT 
                    user_id,
                    MIN(start_time) as first_session_time
                FROM sessions
                WHERE tracking_id = :tracking_id
                GROUP BY user_id
            )
            SELECT 
                COUNT(DISTINCT CASE 
                    WHEN s.start_time = ufs.first_session_time 
                    THEN s.user_id 
                END) as new_users,
                COUNT(DISTINCT CASE 
                    WHEN s.start_time > ufs.first_session_time 
                    THEN s.user_id 
                END) as returning_users
            FROM sessions s
            INNER JOIN user_first_session ufs ON s.user_id = ufs.user_id
            WHERE s.tracking_id = :tracking_id
            AND s.start_time >= now() - INTERVAL 30 DAY
        ";

        $result = $this->client->select($query, ['tracking_id' => $trackingId]);
        $row = $result->rows()[0] ?? ['new_users' => 0, 'returning_users' => 0];

        $newUsers = (int) $row['new_users'];
        $returningUsers = (int) $row['returning_users'];
        $total = $newUsers + $returningUsers;

        return [
            'new_users' => $newUsers,
            'returning_users' => $returningUsers,
            'new_percentage' => $total > 0 ? round($newUsers * 100.0 / $total, 2) : 0,
            'returning_percentage' => $total > 0 ? round($returningUsers * 100.0 / $total, 2) : 0,
        ];

    } catch (\Exception $e) {
        \Log::error('ClickHouse getNewVsReturningUsers error: ' . $e->getMessage());
        return [
            'new_users' => 0,
            'returning_users' => 0,
            'new_percentage' => 0,
            'returning_percentage' => 0,
        ];
    }
}



public function getTopReferrers(string $trackingId, int $limit = 10): array
{
    try {
        $query = "
            SELECT 
                referrer,
                COUNT(DISTINCT session_id) as sessions,
                COUNT(DISTINCT user_id) as unique_users,
                ROUND(COUNT(DISTINCT session_id) * 100.0 / (SELECT COUNT(DISTINCT session_id) FROM sessions WHERE tracking_id = :tracking_id), 2) as percentage
            FROM sessions
            WHERE tracking_id = :tracking_id
            AND referrer != ''
            AND referrer != 'direct'
            GROUP BY referrer
            ORDER BY sessions DESC
            LIMIT :limit
        ";

        $result = $this->client->select($query, [
            'tracking_id' => $trackingId,
            'limit' => $limit
        ]);

        $referrers = [];
        foreach ($result->rows() as $row) {
            $referrers[] = [
                'referrer' => $row['referrer'],
                'sessions' => (int) $row['sessions'],
                'unique_users' => (int) $row['unique_users'],
                'percentage' => (float) $row['percentage'],
            ];
        }

        return $referrers;

    } catch (\Exception $e) {
        \Log::error('ClickHouse getTopReferrers error: ' . $e->getMessage());
        return [];
    }
}





public function countEventsForTracking(string $trackingId): int
{
    $result = $this->client->select(
        "SELECT count() AS cnt FROM page_events WHERE tracking_id = :tracking_id",
        ['tracking_id' => $trackingId]
    );
    $rows = $result->rows();
    return isset($rows[0]['cnt']) ? (int) $rows[0]['cnt'] : 0;
}


public function getLastEventsForTracking(string $trackingId, int $limit = 20): array
{
    $result = $this->client->select(
        "SELECT timestamp, session_id, user_id, tracking_id, event_type, page_url, page_title, referrer
         FROM page_events
         WHERE tracking_id = :tracking_id
         ORDER BY timestamp DESC
         LIMIT :limit",
        ['tracking_id' => $trackingId, 'limit' => $limit]
    );
    return $result->rows() ?? [];
}

public function getSessionsForTracking(string $trackingId, int $limit = 50): array
{
    $result = $this->client->select(
        "SELECT DISTINCT session_id, user_id, tracking_id, start_time, end_time,
                coalesce(duration_ms, 0) / 1000 as duration_sec, 
                page_views, bounce,
                entry_page, exit_page, device_type, operating_system, browser,
                country, country_code
         FROM sessions
         WHERE tracking_id = :tracking_id
         ORDER BY start_time DESC
         LIMIT :limit",
        ['tracking_id' => $trackingId, 'limit' => $limit]
    );
    return $result->rows() ?? [];
}


public function getUserJourneyByTracking(string $trackingId, ?string $sessionId = null): array
{
    try {
        $query = "
            SELECT 
                timestamp,
                session_id,
                page_url,
                page_title,
                referrer,
                duration_ms,
                scroll_depth_max,
                click_count,
                page_load_time,
                ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY timestamp) as step_number
            FROM page_events
            WHERE tracking_id = :tracking_id
        ";

        $params = ['tracking_id' => $trackingId];

        if ($sessionId) {
            $query .= " AND session_id = :session_id";
            $params['session_id'] = $sessionId;
        }

        $query .= " ORDER BY timestamp ASC LIMIT 1000";

        $result = $this->client->select($query, $params);

        $sessions = [];
        foreach ($result->rows() as $row) {
            $sid = $row['session_id'];
            
            if (!isset($sessions[$sid])) {
                $sessions[$sid] = [
                    'session_id' => $sid,
                    'pages' => [],
                    'total_pages' => 0,
                    'total_duration' => 0,
                ];
            }

            $sessions[$sid]['pages'][] = [
                'step' => (int) $row['step_number'],
                'timestamp' => $row['timestamp'],
                'page_url' => $row['page_url'],
                'page_title' => $row['page_title'],
                'referrer' => $row['referrer'],
                'duration_ms' => (int) $row['duration_ms'],
                'scroll_depth' => (float) $row['scroll_depth_max'],
                'clicks' => (int) $row['click_count'],
                'load_time_ms' => (int) $row['page_load_time'],
            ];

            $sessions[$sid]['total_pages']++;
            $sessions[$sid]['total_duration'] += (int) $row['duration_ms'];
        }

        return [
            'tracking_id' => $trackingId,
            'total_sessions' => count($sessions),
            'sessions' => array_values($sessions),
        ];

    } catch (\Exception $e) {
        \Log::error('ClickHouse getUserJourneyByTracking error: ' . $e->getMessage());
        return [
            'tracking_id' => $trackingId,
            'total_sessions' => 0,
            'sessions' => [],
        ];
    }
}

}
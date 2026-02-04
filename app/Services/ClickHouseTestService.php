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
            "password" => env("CLICKHOUSE_PASSWORD", ""),
        ]);

        // Database
        $this->client->database(env("CLICKHOUSE_DATABASE", "default"));
    }

    public function insertEvent(array $data)
{
    $this->client->insert("page_events", [[
        "timestamp"           => $data["timestamp"],
        "session_id"          => $data["session_id"],
        "user_id"             => $data["user_id"],
        "tracking_id"         => $data["tracking_id"],
        "event_type"          => $data["event_type"],
        "page_url"            => $data["page_url"],
        "page_title"          => $data["page_title"],
        "referrer"            => $data["referrer"],

        "duration_ms"         => $data["duration_ms"],
        "scroll_depth_max"    => $data["scroll_depth_max"],
        "click_count"         => $data["click_count"],

        "dns_time"            => $data["dns_time"],
        "connect_time"        => $data["connect_time"],
        "response_time"       => $data["response_time"],
        "dom_load_time"       => $data["dom_load_time"],
        "page_load_time"      => $data["page_load_time"],

        "connection_type"     => $data["connection_type"],
        "connection_downlink" => $data["connection_downlink"],
        "connection_rtt"      => $data["connection_rtt"],
        "save_data"           => $data["save_data"],
    ]]);
}

    public function latestEvents(int $limit = 10)
    {
        $events = $this->client->select("
            SELECT *
            FROM page_events
            ORDER BY timestamp DESC
            LIMIT $limit
        ");

    return response()->json($events->rows());
}

}

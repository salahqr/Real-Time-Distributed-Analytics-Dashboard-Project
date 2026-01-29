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

        $this->client->database(env("CLICKHOUSE_DATABASE", "analytics"));
    }

    public function insertEvent(array $data)
    {
        $this->client->insert(
            "page_events (timestamp, session_id, user_id, tracking_id, event_type, page_url)",
            [[
                now()->toDateTimeString(),
                $data["session_id"],
                $data["user_id"],
                $data["tracking_id"],
                $data["event_type"],
                $data["page_url"],
            ]]
        );
    }
}

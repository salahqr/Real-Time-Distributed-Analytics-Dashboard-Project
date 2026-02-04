<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use App\Services\ClickHouseTestService;

class AnalyticsController extends Controller
{
    protected ClickHouseTestService $clickhouse;

    public function __construct(ClickHouseTestService $clickhouse)
    {
        $this->clickhouse = $clickhouse;
    }

    /**
     * Store Tracking Event
     */
    public function track(Request $request)
    {
        $request->validate([
            "tracking_id" => "required|string",
            "page_url"    => "required|string",
            "event_type"  => "required|string",
        ]);

        $this->clickhouse->insertEvent([
            "timestamp"        => now()->toDateTimeString(),
            "session_id"       => $request->session_id ?? ("sess_" . uniqid()),
            "user_id"          => auth()->id() ? strval(auth()->id()) : "guest",

            "tracking_id"      => $request->tracking_id,
            "event_type"       => $request->event_type,
            "page_url"         => $request->page_url,

            // ✅ قيم حقيقية بدل NULL
            "page_title"       => $request->page_title ?? "Homepage",
            "referrer"         => $request->referrer ?? "direct",

            "duration_ms"      => $request->duration_ms ?? 0,
            "scroll_depth_max" => $request->scroll_depth_max ?? 0,
            "click_count"      => $request->click_count ?? 0,

            "dns_time"         => $request->dns_time ?? 0,
            "connect_time"     => $request->connect_time ?? 0,
            "response_time"    => $request->response_time ?? 0,
            "dom_load_time"    => $request->dom_load_time ?? 0,
            "page_load_time"   => $request->page_load_time ?? 0,

            "connection_type"  => $request->connection_type ?? "wifi",
            "connection_downlink" => $request->connection_downlink ?? 10,
            "connection_rtt"   => $request->connection_rtt ?? 50,
            "save_data"        => $request->save_data ?? 0,
        ]);

        return response()->json([
            "status"  => "ok",
            "message" => "Event stored successfully"
        ]);
    }

    /**
     * Latest Events
     */
    public function latest()
    {
        return response()->json(
            $this->clickhouse->latestEvents(10)
        );
    }
}

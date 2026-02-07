<?php

namespace App\Http\Controllers;

use Illuminate\Http\Request;
use Illuminate\Support\Facades\Auth;
use App\Services\ClickHouseTestService;

class AnalyticsController extends Controller
{
    protected ClickHouseTestService $clickhouse;

    public function __construct(ClickHouseTestService $clickhouse)
    {
        $this->clickhouse = $clickhouse;
    }

   public function track(Request $request)
{
    $request->validate([
        "tracking_id" => "required|string",
        "page_url"    => "required|string",
        "event_type"  => "required|string",
    ]);

    $this->clickhouse->insertEvent([
        "tracking_id" => $request->tracking_id,
        "page_url"    => $request->page_url,
        "event_type"  => $request->event_type,

        "user_id"     =>  Auth::check() ? Auth::id() : "guest",
        "session_id"  => $request->session_id ?? ("sess_" . uniqid()),
    ]);

    return response()->json([
        "status"  => "ok",
        "message" => "Event stored successfully"
    ]);
}

}

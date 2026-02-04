<?php

namespace App\Http\Controllers;

use App\Models\User;
use App\Services\ClickHouseTestService;

class UserAnalyticsController extends Controller
{
    public function show(string $id, ClickHouseTestService $clickhouse)
    {
        // ✅ User من MySQL
        $user = User::findOrFail($id);

        // ✅ Events Count من ClickHouse
        $eventsCount = $clickhouse->countEventsForUser($user->id);

        // ✅ Last Events من ClickHouse
        $lastEvents = $clickhouse->getLastEventsForUser($user->id);

        return response()->json([
            "user" => $user,
            "events_count" => $eventsCount,
            "last_events" => $lastEvents
        ]);
    }
}

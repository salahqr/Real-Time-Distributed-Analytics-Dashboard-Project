<?php
use App\Services\ClickHouseTestService;
use Illuminate\Support\Facades\Route;

Route::get('/reset-password/{token}', function (string $token) {
    return response()->json([
        'token' => $token,
        'email' => request('email'),
    ]);
})->name('password.reset');


use App\Services\ClickHouseService;

Route::get('/test-clickhouse', function (ClickHouseTestService $service) {
    $service->testInsert();
    return 'ClickHouse insert OK';
});

use App\Events\RealtimeEvent;

Route::get('/send-event', function () {

    broadcast(new RealtimeEvent("🔥 حدث جديد من Laravel عبر WebSocket"));

    return "Event Broadcasted Successfully!";
});

use App\Events\TestEvent;

Route::get('/fire-event', function () {
    broadcast(new TestEvent("Hello WebSocket 🚀"));

    return "Event Broadcasted Successfully!";
});


use App\Events\MessageEvent;

Route::get('/send-message', function () {
    broadcast(new MessageEvent("Hello from Laravel فقط 🚀"));

    return "Message Sent!";
});


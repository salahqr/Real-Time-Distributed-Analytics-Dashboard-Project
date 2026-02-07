<?php

use Illuminate\Support\Facades\Route;
use Illuminate\Foundation\Auth\EmailVerificationRequest;
use Illuminate\Support\Facades\Mail;

use App\Models\User;
use App\Http\Controllers\AuthController;
use App\Http\Controllers\PasswordResetController;
use App\Http\Controllers\AnalyticsController;
use App\Http\Controllers\UserAnalyticsController;
use App\Services\ClickHouseTestService;
use App\Events\UserLoggedIn;

/*
|--------------------------------------------------------------------------
| Public Auth Routes
|--------------------------------------------------------------------------
*/

Route::post('/register', [AuthController::class, 'register']);
Route::post('/login',    [AuthController::class, 'login']);

/*
|--------------------------------------------------------------------------
| Password Reset Routes
|--------------------------------------------------------------------------
*/

Route::post('/password/forgot', [PasswordResetController::class, 'sendResetLink']);
Route::post('/password/reset',  [PasswordResetController::class, 'resetPassword']);

/*
|--------------------------------------------------------------------------
| Email Verification
|--------------------------------------------------------------------------
*/

Route::get('/email/verify/{id}/{hash}', function (EmailVerificationRequest $request) {
    $request->fulfill();
    return response()->json(['message' => 'Email verified successfully']);
})->middleware(['signed', 'throttle:6,1'])->name('verification.verify');

/*
|--------------------------------------------------------------------------
| Protected Routes (JWT)
|--------------------------------------------------------------------------
*/

Route::middleware('jwt.auth')->group(function () {

    Route::post('/logout',  [AuthController::class, 'logout']);
    Route::post('/refresh', [AuthController::class, 'refresh']);
    Route::get('/me',       [AuthController::class, 'me']);

});

/*
|--------------------------------------------------------------------------
| Admin Only
|--------------------------------------------------------------------------
*/

Route::middleware(['jwt.auth', 'role:admin'])->get('/admin-only', function () {
    return response()->json(['message' => 'Welcome Admin']);
});

/*
|--------------------------------------------------------------------------
| Analytics Tracking (ClickHouse)
|--------------------------------------------------------------------------
*/

Route::post('/track', [AnalyticsController::class, 'track']);

/*
|--------------------------------------------------------------------------
| User Analytics Dashboard
|--------------------------------------------------------------------------
*/

Route::get('/user/{id}/analytics', [UserAnalyticsController::class, 'show']);

/*
|--------------------------------------------------------------------------
| DEV / LOCAL ONLY
|--------------------------------------------------------------------------
*/

if (app()->environment('local')) {

    Route::get('/test-clickhouse', function (ClickHouseTestService $service) {

        $service->insertEvent([
            "timestamp"   => now()->toDateTimeString(),
            "session_id"  => "sess_test",
            "user_id"     => "guest",
            "tracking_id" => "site_test",
            "event_type"  => "page_view",
            "page_url"    => "/test",
            "page_title"  => "Test Page",
            "referrer"    => "direct",
        ]);

        return "ClickHouse insert OK";
    });

    Route::get('/test-broadcast', function () {
        event(new UserLoggedIn('test@example.com'));
        return response()->json(['status' => 'event sent']);
    });

    Route::get('/test-mail', function () {
        Mail::raw("Laravel Test Email ✅", function ($m) {
            $m->to("malmasri345@gmail.com")
              ->subject("Laravel Mail Test");
        });
        return "Mail sent";
    });

    Route::get('/users', function () {
        return User::all();
    });
}

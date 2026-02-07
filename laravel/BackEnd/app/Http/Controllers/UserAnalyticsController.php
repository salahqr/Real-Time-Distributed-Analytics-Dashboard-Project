<?php

namespace App\Http\Controllers;

use App\Models\User;
use App\Services\ClickHouseTestService;

class UserAnalyticsController extends Controller
{
    public function show(string $id, ClickHouseTestService $clickhouse)
    {
       
        $user = User::findOrFail($id);

        $trackingId = $user->id;

      
        $realtime = [
            'online_users' => $clickhouse->getOnlineUsersCount($trackingId),
            'live_page_views' => $clickhouse->getLivePageViews(5, $trackingId),
            'interactions' => $clickhouse->getLiveInteractions($trackingId),
            'form_submissions' => $clickhouse->getLiveFormSubmissions($trackingId),
            'user_locations' => $clickhouse->getLiveUserLocations($trackingId),
            'popular_pages' => $clickhouse->getLivePopularPages($trackingId),
            'timestamp' => now()->toIso8601String(),
        ];

      
        $pages = [
            'analytics' => $clickhouse->getPageAnalytics($trackingId, '1h'),
            'bounce_rate' => $clickhouse->getBounceRateByHour($trackingId),
        ];

        $sessions = $clickhouse->getSessionsForUser($trackingId, 50);

       
        $devices = [
            'breakdown' => $clickhouse->getDeviceBreakdown($trackingId),
            'resolutions' => $clickhouse->getScreenResolutions($trackingId),
            'mobile_vs_desktop' => $clickhouse->getMobileVsDesktop($trackingId),
            'geographic' => $clickhouse->getGeographicData($trackingId),
            'connection_quality' => $clickhouse->getConnectionQuality($trackingId),
        ];

     
        $behavior = [
            'scroll_depth' => $clickhouse->getScrollDepthAnalysis($trackingId),
            'form_patterns' => $clickhouse->getFormPatterns($trackingId),
            'downloads_clicks' => $clickhouse->getDownloadsAndClicks($trackingId),
            'video_engagement' => $clickhouse->getVideoEngagement($trackingId),
        ];

   
        $ecommerce = [
            'products' => $clickhouse->getProductAnalytics($trackingId),
            'cart' => $clickhouse->getCartAnalytics($trackingId),

        ];
  


  $journey = $clickhouse->getUserJourney($trackingId);

    
        $traffic = [
            'sources' => $clickhouse->getTrafficSourcesBreakdown($trackingId),
            'entry_exit' => $clickhouse->getEntryExitPages($trackingId),
            'avg_session_duration' => $clickhouse->getAverageSessionDuration($trackingId),
            'page_load_performance' => $clickhouse->getPageLoadPerformance($trackingId),
            'browsers' => $clickhouse->getBrowserBreakdown($trackingId),
            'operating_systems' => $clickhouse->getOperatingSystemBreakdown($trackingId),
            'hourly_pattern' => $clickhouse->getHourlyTrafficPattern($trackingId),
            'top_referrers' => $clickhouse->getTopReferrers($trackingId, 10),
        ];

      
        $user_metrics = [
            'daily_active_users' => $clickhouse->getDailyActiveUsers($trackingId, 30),
            'new_vs_returning' => $clickhouse->getNewVsReturningUsers($trackingId),
        ];

        
       
        return response()->json([
            'user' => $user,
            'realtime' => $realtime,
            'events_count' => $clickhouse->countEventsForTracking($trackingId),
            'last_events' => $clickhouse->getLastEventsForTracking($trackingId, 20),
            'sessions' => $clickhouse->getSessionsForTracking($trackingId, 50),
            'journey' => $clickhouse->getUserJourneyByTracking($trackingId),
            'pages' => $pages,
            'devices' => $devices,
            'behavior' => $behavior,
            'ecommerce' => $ecommerce,
            'traffic' => $traffic,
            'user_metrics' => $user_metrics,
        ]);
    }
}
import type { MeResponse } from "../services/auth";


export interface UnifiedAnalyticsResponse {
  user: MeResponse;

  realtime: RealtimeData;

  events_count: number;
  last_events: AnalyticsEventRow[];

  pages: PageDataSection;

  sessions: SessionInfo[];

  devices: DeviceDataSection;

  behavior: BehaviorDataSection;

  ecommerce: EcommerceDataSection;

  journey?: UserJourneyData; 

  traffic?: TrafficDataSection; 

  user_metrics?: UserMetricsSection; 

  custom_events?: CustomEventData[];
  errors?: ErrorEventData[]; 
}


export interface UserJourneyData {
  user_id: string;
  total_sessions: number;
  sessions: UserJourneySession[];
}

export interface UserJourneySession {
  session_id: string;
  total_pages: number;
  total_duration: number;
  pages: JourneyPage[];
}

export interface JourneyPage {
  step: number;
  timestamp: string;
  page_url: string;
  page_title: string;
  referrer: string;
  duration_ms: number;
  scroll_depth: number;
  clicks: number;
  load_time_ms: number;
}


export interface TrafficDataSection {
  sources: TrafficSource[];
  entry_exit: EntryExitPages[];
  avg_session_duration: number;
  page_load_performance: PageLoadPerformance[];
  browsers: BrowserBreakdown[];
  operating_systems: OSBreakdown[];
  hourly_pattern: HourlyTraffic[];
  top_referrers: TopReferrer[];
}

export interface TrafficSource {
  source_type: string; 
  referrer: string;
  sessions: number;
  unique_users: number;
  pageviews: number;
  avg_session_duration_sec: number;
  bounce_rate: number;
}

export interface EntryExitPages {
  page_type: "entry" | "exit";
  page_url: string;
  page_title: string;
  sessions: number;
  percentage: number;
}

export interface PageLoadPerformance {
  page_url: string;
  avg_load_time_ms: number;
  median_load_time_ms: number;
  p90_load_time_ms: number;
  p95_load_time_ms: number;
  samples: number;
}

export interface BrowserBreakdown {
  browser: string;
  sessions: number;
  unique_users: number;
  pageviews: number;
  percentage: number;
}

export interface OSBreakdown {
  operating_system: string;
  sessions: number;
  unique_users: number;
  pageviews: number;
  percentage: number;
}

export interface HourlyTraffic {
  hour: number; // 0-23
  sessions: number;
  unique_users: number;
  pageviews: number;
  avg_session_duration_sec: number;
}

export interface TopReferrer {
  referrer: string;
  sessions: number;
  unique_users: number;
  percentage: number;
}


export interface UserMetricsSection {
  daily_active_users: DailyActiveUsers[];
  new_vs_returning: NewVsReturning;
  retention: UserRetention[];
}

export interface DailyActiveUsers {
  date: string;
  active_users: number;
  new_users: number;
  returning_users: number;
}

export interface NewVsReturning {
  new_users: number;
  returning_users: number;
  new_percentage: number;
  returning_percentage: number;
}

export interface UserRetention {
  cohort_date: string;
  users: number;
  day_0: number;
  day_1: number;
  day_3: number;
  day_7: number;
  day_14: number;
  day_30: number;
}


export interface CustomEventData {
  event_type: string;
  event_count: number;
  unique_users: number;
  avg_value?: number;
  pages: string[];
}

export interface ErrorEventData {
  timestamp: string;
  session_id: string;
  user_id: string;
  page_url: string;
  error_message: string;
  error_stack?: string;
  browser: string;
  operating_system: string;
}


export interface EcommerceDataSection {
 
  conversion_rate?: number;
  revenue_metrics?: RevenueMetrics;
  avg_order_value?: number;
  cart_abandonment_rate?: number;
}

export interface RevenueMetrics {
  total_revenue: number;
  total_orders: number;
  avg_order_value: number;
  unique_customers: number;
  revenue_by_day?: DailyRevenue[];
}

export interface DailyRevenue {
  date: string;
  orders: number;
  revenue: number;
  avg_order_value: number;
}


export interface VideoEventDetail {
  timestamp: string;
  session_id: string;
  user_id: string;
  page_url: string;
  video_src: string;
  event_type: "play" | "pause" | "ended" | "progress";
  current_time_sec?: number;
  duration_sec?: number;
}

export interface FileDownload {
  timestamp: string;
  session_id: string;
  user_id: string;
  page_url: string;
  file_url: string;
  file_name: string;
  file_type: string;
}


export interface SearchTerm {
  search_term: string;
  searches: number;
  unique_users: number;
  avg_results?: number;
  pages: string[];
}

export interface RealtimeData {
  online_users: number;
  live_page_views: number;
  interactions: InteractionSummary[];
  form_submissions: FormSubmissionSummary[];
  user_locations: UserLocation[];
  popular_pages: PopularPage[];
  timestamp: string;
}

export interface InteractionSummary {
  event_type: string;
  count: number;
  unique_users: number;
}

export interface FormSubmissionSummary {
  form_id: string;
  form_name: string;
  submissions: number;
  successful: number;
}

export interface UserLocation {
  country: string;
  country_code: string;
  users: number;
}

export interface PopularPage {
  page_url: string;
  views: number;
  unique_visitors: number;
  avg_time_sec: number;
}


export interface AnalyticsEventRow {
  timestamp: string;
  session_id?: string | null;
  user_id?: string | number | null;
  tracking_id: string;
  event_type: string;
  page_url: string;
  [key: string]: any;
}

export interface PageDataSection {
  analytics: PageAnalytics[];
  bounce_rate: BounceRate[];
}

export interface PageAnalytics {
  page_url: string;
  page_title: string;
  pageviews: number;
  unique_visitors: number;
  avg_time_on_page_sec: number;
  avg_scroll_depth: number;
  total_clicks: number;
  avg_load_time_ms: number;
}

export interface BounceRate {
  hour: string;
  total_sessions: number;
  bounce_sessions: number;
  bounce_rate: number;
}


export interface SessionInfo {
  session_id: string;
  user_id: string;
  start_time: string;
  end_time: string | null;
  duration_sec: number;
  page_views: number;
  bounce: number;
  entry_page: string;
  exit_page: string | null;
}


export interface DeviceDataSection {
  breakdown: DeviceBreakdown[];
  resolutions: ScreenResolution[];
  mobile_vs_desktop: MobileVsDesktop[];
  geographic: GeographicData[];
  connection_quality: ConnectionQuality[];
}

export interface DeviceBreakdown {
  device_type: string;
  operating_system: string;
  browser: string;
  sessions: number;
  unique_users: number;
  pageviews: number;
  avg_session_duration_sec: number;
  bounce_rate: number;
}

export interface ScreenResolution {
  resolution: string;
  sessions: number;
  unique_users: number;
}

export interface MobileVsDesktop {
  device_type: string;
  sessions: number;
  unique_users: number;
  pageviews: number;
  percentage: number;
}

export interface GeographicData {
  country: string;
  country_code: string;
  sessions: number;
  unique_users: number;
  pageviews: number;
  avg_session_duration_sec: number;
}

export interface ConnectionQuality {
  connection_type: string;
  events: number;
  avg_downlink_mbps: number;
  avg_rtt_ms: number;
  save_data_enabled: number;
}


export interface BehaviorDataSection {
  scroll_depth: ScrollDepthAnalysis[];
  form_patterns: FormPattern[];
  downloads_clicks: DownloadClick[];
  video_engagement: VideoEngagement[];
}

export interface ScrollDepthAnalysis {
  page_url: string;
  avg_scroll_depth: number;
  median_scroll_depth: number;
  p90_scroll_depth: number;
  sessions: number;
}

export interface FormPattern {
  form_id: string;
  form_name: string;
  form_starts: number;
  field_interactions: number;
  submissions: number;
  successful_submissions: number;
  conversion_rate: number;
}

export interface DownloadClick {
  event_type: string;
  resource: string;
  link_text: string | null;
  is_external: number;
  clicks: number;
  unique_users: number;
}

export interface VideoEngagement {
  video_src: string;
  plays: number;
  completions: number;
  completion_rate: number;
  watched_25_percent: number;
  watched_50_percent: number;
  watched_75_percent: number;
  unique_viewers: number;
  avg_video_duration_sec: number;
}


export const isUnifiedAnalyticsResponse = (
  data: any,
): data is UnifiedAnalyticsResponse => {
  return (
    typeof data === "object" &&
    data.user !== undefined &&
    data.realtime !== undefined &&
    data.pages !== undefined &&
    data.devices !== undefined &&
    data.behavior !== undefined &&
    data.ecommerce !== undefined
  );
};

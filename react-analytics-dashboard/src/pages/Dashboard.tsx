import React, { useEffect, useState, useMemo } from "react";
import { NavLink, useNavigate, useSearchParams } from "react-router-dom";
import "./Dashboard.scss";
import { useAuth } from "../components/AuthContext";
import { getUserAnalytics } from "../services/analytics";
import type { UnifiedAnalyticsResponse } from "../models/analytics.models";

const REALTIME_INTERVAL = 5000;

const formatToJordanTime = (utcTimestamp?: string): string => {
  if (!utcTimestamp) return "—";

  try {
    const timestamp = utcTimestamp.endsWith("Z")
      ? utcTimestamp
      : utcTimestamp + "Z";
    const date = new Date(timestamp);

    if (isNaN(date.getTime())) return "—";

    return new Intl.DateTimeFormat("ar-JO", {
      timeZone: "Asia/Amman",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
      hour12: false,
    }).format(date);
  } catch (error) {
    console.error("Date formatting error:", error);
    return "—";
  }
};

export default function Dashboard() {
  const { me } = useAuth();
  const [sp] = useSearchParams();
  const targetId = sp.get("user") || me?.id || "";

  const [data, setData] = useState<UnifiedAnalyticsResponse | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [activeTab, setActiveTab] = useState<string>("overview");
  const navigate = useNavigate();

  useEffect(() => {
    if (!targetId) return;

    let mounted = true;

    const fetchData = async () => {
      try {
        const res = await getUserAnalytics(targetId);
        if (mounted) {
          setData(res);
          setError(null);
          setLoading(false);
        }
      } catch (err: any) {
        if (mounted) {
          setError(err?.response?.data?.message || "Failed to load analytics");
          setLoading(false);
        }
      }
    };

    fetchData();
    const interval = setInterval(fetchData, REALTIME_INTERVAL);

    return () => {
      mounted = false;
      clearInterval(interval);
    };
  }, [targetId]);

  const user = data?.user;
  const realtime = data?.realtime;
  const pages = data?.pages;
  const sessions = data?.sessions || [];
  const devices = data?.devices;
  const behavior = data?.behavior;
  const ecommerce = data?.ecommerce;
  const lastEvents = data?.last_events || [];
  const eventsCount = data?.events_count || 0;


  const { logout } = useAuth();

  const onLogout = async () => {
    await logout();
    navigate("/login");
  };

  return (
    <div className="dash-root">
      <header className="dash-header">
        <div className="logo-area">
          <span className="logo-mark" />
          <span className="logo-text">DataFlow Analytics</span>
        </div>

        <nav className="top-nav">
          <NavLink
            to="/dashboard"
            className={({ isActive }) => (isActive ? "active" : "")}
          >
            Dashboard
          </NavLink>
          <NavLink
            to="/setup"
            className={({ isActive }) => (isActive ? "active" : "")}
          >
            Integration
          </NavLink>
          <a
            href="#"
            className="logout"
            onClick={(e) => {
              e.preventDefault();
              onLogout();
            }}
          >
            <i className="fas fa-sign-out-alt"></i> Logout
          </a>
        </nav>
      </header>

      <main className="dash-wrap">
        <div className="dash-title-row">
          <div className="dash-title-col">
            <h1 className="dash-title">Analytics Dashboard</h1>
            <p className="dash-sub">
              Real-time insights and comprehensive metrics
            </p>
          </div>

          <div className="dash-meta">
            <div className="pill">
              <span className="pill-label">Target User</span>
              <span className="pill-value mono">{targetId || "—"}</span>
            </div>
            <div className="pill">
              <span className="pill-label">Total Events</span>
              <span className="pill-value">{eventsCount.toLocaleString()}</span>
            </div>
            <div className="pill">
              <span className="pill-label">Online Users</span>
              <span className="pill-value">{realtime?.online_users || 0}</span>
            </div>
            <div className="pill">
              <span className="pill-label">Live Page Views</span>
              <span className="pill-value">
                {realtime?.live_page_views || 0}
              </span>
            </div>
          </div>
        </div>

        {loading && <div className="dash-notice">Loading analytics...</div>}
        {error && <div className="dash-notice error">{error}</div>}

        {!loading && data && (
          <>
            <section className="realtime-section">
              <h2 className="section-title">
                <span className="pulse-dot"></span>
                Real-Time (Live)
              </h2>

              <div className="realtime-grid">
                <StatCard
                  label="Online Users"
                  value={realtime?.online_users || 0}
                  icon="👥"
                />
                <StatCard
                  label="Page Views (5s)"
                  value={realtime?.live_page_views || 0}
                  icon="📄"
                />
                <StatCard
                  label="Interactions"
                  value={
                    realtime?.interactions?.reduce(
                      (sum, i) => sum + (i.count || 0),
                      0,
                    ) || 0
                  }
                  icon="🎯"
                />
                <StatCard
                  label="Form Submissions"
                  value={
                    realtime?.form_submissions?.reduce(
                      (sum, f) => sum + (f.submissions || 0),
                      0,
                    ) || 0
                  }
                  icon="📝"
                />
              </div>

              {realtime?.popular_pages && realtime.popular_pages.length > 0 && (
                <div className="popular-pages-now">
                  <h2 className="section-title">Popular Pages (Last 5 min)</h2>
                  <div className="page-list">
                    {realtime.popular_pages.slice(0, 5).map((page, i) => (
                      <div key={i} className="page-item">
                        <span className="page-url">{page.page_url}</span>
                        <div className="page-stats">
                          <span className="stat">{page.views} views</span>
                          <span className="stat">
                            {page.unique_visitors} visitors
                          </span>
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              )}
            </section>

            <div className="tabs">
              <button
                className={activeTab === "overview" ? "active" : ""}
                onClick={() => setActiveTab("overview")}
              >
                Overview
              </button>
              <button
                className={activeTab === "pages" ? "active" : ""}
                onClick={() => setActiveTab("pages")}
              >
                📈 Pages
              </button>
              <button
                className={activeTab === "sessions" ? "active" : ""}
                onClick={() => setActiveTab("sessions")}
              >
                🕐 Sessions
              </button>
              <button
                className={activeTab === "devices" ? "active" : ""}
                onClick={() => setActiveTab("devices")}
              >
                💻 Devices
              </button>
              <button
                className={activeTab === "behavior" ? "active" : ""}
                onClick={() => setActiveTab("behavior")}
              >
                🎯 Behavior
              </button>
              <button
                className={activeTab === "ecommerce" ? "active" : ""}
                onClick={() => setActiveTab("ecommerce")}
              >
                🛍️ E-commerce
              </button>
              <button
                className={activeTab === "events" ? "active" : ""}
                onClick={() => setActiveTab("events")}
              >
                📋 Events
              </button>
            </div>

            <div className="tab-content">
              {activeTab === "overview" && (
                <OverviewTab data={{ sessions, user }} />
              )}
              {activeTab === "pages" && <PagesTab data={pages} />}
              {activeTab === "sessions" && <SessionsTab data={sessions} />}
              {activeTab === "devices" && <DevicesTab data={devices} />}
              {activeTab === "behavior" && <BehaviorTab data={behavior} />}
              {activeTab === "ecommerce" && <EcommerceTab data={ecommerce} />}
              {activeTab === "events" && <EventsTab events={lastEvents} />}
            </div>
          </>
        )}
      </main>
    </div>
  );
}


function StatCard({
  label,
  value,
  icon,
}: {
  label: string;
  value: number;
  icon: string;
}) {
  return (
    <div className="stat-card">
      <div className="stat-icon">{icon}</div>
      <div className="stat-content">
        <div className="stat-label">{label}</div>
        <div className="stat-value">{Number(value || 0).toLocaleString()}</div>
      </div>
    </div>
  );
}


function OverviewTab({ data }: any) {
  const sessions = data?.sessions || [];
  const user = data?.user;

  return (
    <div className="overview-content">
      <section className="kpis">
        <div className="kpi-card">
          <div className="kpi-label">User Name</div>
          <div className="kpi-value small">{user?.name || "—"}</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">User Email</div>
          <div className="kpi-value small">{user?.email || "—"}</div>
        </div>
        <div className="kpi-card">
          <div className="kpi-label">Total Sessions</div>
          <div className="kpi-value">{sessions.length}</div>
        </div>
      </section>

      <div className="table-card">
        <h3>Recent Sessions</h3>
        <div className="table-wrapper">
          <table className="data-table">
            <thead>
              <tr>
                <th>Session ID</th>
                <th>Start Time</th>
                <th>Duration</th>
                <th>Page Views</th>
                <th>Bounce</th>
                <th>Entry Page</th>
              </tr>
            </thead>
            <tbody>
              {sessions.slice(0, 20).map((s: any, i: number) => (
                <tr key={i}>
                  <td className="mono">{s.session_id}</td>
                  <td>{formatToJordanTime(s.start_time)}</td>
                  <td>{Math.round(s.duration_sec || 0)}s</td>
                  <td>{s.page_views}</td>
                  <td>{s.bounce ? "Yes" : "No"}</td>
                  <td className="truncate">{s.entry_page}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}


function PagesTab({ data }: any) {
  const analytics = data?.analytics || [];

  return (
    <div className="table-card">
      <h3>Page Analytics</h3>
      <div className="table-wrapper">
        <table className="data-table">
          <thead>
            <tr>
              <th>Page URL</th>
              <th>Views</th>
              <th>Unique</th>
              <th>Avg Time</th>
              <th>Scroll %</th>
              <th>Clicks</th>
              <th>Load Time</th>
            </tr>
          </thead>
          <tbody>
            {analytics.map((p: any, i: number) => (
              <tr key={i}>
                <td className="truncate">{p.page_url}</td>
                <td>{p.pageviews?.toLocaleString()}</td>
                <td>{p.unique_visitors?.toLocaleString()}</td>
                <td>{Math.round(p.avg_time_on_page_sec || 0)}s</td>
                <td>{Math.round(p.avg_scroll_depth || 0)}%</td>
                <td>{p.total_clicks?.toLocaleString()}</td>
                <td>{Math.round(p.avg_load_time_ms || 0)}ms</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}

function SessionsTab({ data }: any) {
  return (
    <div className="table-card">
      <h3>All Sessions</h3>
      <div className="table-wrapper">
        <table className="data-table">
          <thead>
            <tr>
              <th>Session ID</th>
              <th>Start Time</th>
              <th>End Time</th>
              <th>Duration</th>
              <th>Page Views</th>
              <th>Bounce</th>
              <th>Entry</th>
              <th>Exit</th>
            </tr>
          </thead>
          <tbody>
            {data.map((s: any, i: number) => (
              <tr key={i}>
                <td className="mono">{s.session_id}</td>
                <td>{formatToJordanTime(s.start_time)}</td>
                <td>{s.end_time ? formatToJordanTime(s.end_time) : "—"}</td>
                <td>{Math.round(s.duration_sec || 0)}s</td>
                <td>{s.page_views}</td>
                <td>{s.bounce ? "Yes" : "No"}</td>
                <td className="truncate">{s.entry_page}</td>
                <td className="truncate">{s.exit_page || "—"}</td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}


function DevicesTab({ data }: any) {
  const breakdown = data?.breakdown || [];
  const mobileVsDesktop = data?.mobile_vs_desktop || [];
  const geographic = data?.geographic || [];

  return (
    <div className="devices-content">
      <div className="grid-2">
        <div className="table-card">
          <h3>Device Breakdown</h3>
          <div className="table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Device</th>
                  <th>OS</th>
                  <th>Browser</th>
                  <th>Sessions</th>
                  <th>Bounce %</th>
                </tr>
              </thead>
              <tbody>
                {breakdown.slice(0, 15).map((d: any, i: number) => (
                  <tr key={i}>
                    <td>{d.device_type}</td>
                    <td>{d.operating_system}</td>
                    <td>{d.browser}</td>
                    <td>{d.sessions?.toLocaleString()}</td>
                    <td>{Math.round(d.bounce_rate || 0)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="table-card">
          <h3>Mobile vs Desktop</h3>
          <div className="table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Type</th>
                  <th>Sessions</th>
                  <th>%</th>
                </tr>
              </thead>
              <tbody>
                {mobileVsDesktop.map((m: any, i: number) => (
                  <tr key={i}>
                    <td>{m.device_type}</td>
                    <td>{m.sessions?.toLocaleString()}</td>
                    <td>{Math.round(m.percentage || 0)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      <div className="table-card">
        <h3>Geographic Locations</h3>
        <div className="table-wrapper">
          <table className="data-table">
            <thead>
              <tr>
                <th>Country</th>
                <th>Sessions</th>
                <th>Users</th>
                <th>Pageviews</th>
              </tr>
            </thead>
            <tbody>
              {geographic.slice(0, 20).map((g: any, i: number) => (
                <tr key={i}>
                  <td>
                    {g.country_code} {g.country}
                  </td>
                  <td>{g.sessions?.toLocaleString()}</td>
                  <td>{g.unique_users?.toLocaleString()}</td>
                  <td>{g.pageviews?.toLocaleString()}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  );
}


function BehaviorTab({ data }: any) {
  const scrollDepth = data?.scroll_depth || [];
  const formPatterns = data?.form_patterns || [];
  const videoEngagement = data?.video_engagement || [];

  return (
    <div className="behavior-content">
      <div className="grid-2">
        <div className="table-card">
          <h3>Scroll Depth Analysis</h3>
          <div className="table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Page</th>
                  <th>Avg %</th>
                  <th>Median %</th>
                  <th>Sessions</th>
                </tr>
              </thead>
              <tbody>
                {scrollDepth.slice(0, 10).map((s: any, i: number) => (
                  <tr key={i}>
                    <td className="truncate">{s.page_url}</td>
                    <td>{Math.round(s.avg_scroll_depth || 0)}%</td>
                    <td>{Math.round(s.median_scroll_depth || 0)}%</td>
                    <td>{s.sessions?.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>

        <div className="table-card">
          <h3>Form Patterns</h3>
          <div className="table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Form</th>
                  <th>Starts</th>
                  <th>Submits</th>
                  <th>Rate %</th>
                </tr>
              </thead>
              <tbody>
                {formPatterns.map((f: any, i: number) => (
                  <tr key={i}>
                    <td>{f.form_name}</td>
                    <td>{f.form_starts?.toLocaleString()}</td>
                    <td>{f.submissions?.toLocaleString()}</td>
                    <td>{Math.round(f.conversion_rate || 0)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      </div>

      {videoEngagement.length > 0 && (
        <div className="table-card">
          <h3>Video Engagement</h3>
          <div className="table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Video</th>
                  <th>Plays</th>
                  <th>Complete %</th>
                  <th>Viewers</th>
                </tr>
              </thead>
              <tbody>
                {videoEngagement.map((v: any, i: number) => (
                  <tr key={i}>
                    <td className="truncate">{v.video_src}</td>
                    <td>{v.plays?.toLocaleString()}</td>
                    <td>{Math.round(v.completion_rate || 0)}%</td>
                    <td>{v.unique_viewers?.toLocaleString()}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}


function EcommerceTab({ data }: any) {
  const products = data?.products || [];
  const revenue = data?.revenue || [];
  const conversionFunnel = data?.conversion_funnel || [];

  return (
    <div className="ecommerce-content">
      <div className="table-card">
        <h3>Product Performance</h3>
        <div className="table-wrapper">
          <table className="data-table">
            <thead>
              <tr>
                <th>Product</th>
                <th>Views</th>
                <th>Cart Adds</th>
                <th>Purchases</th>
                <th>Revenue</th>
              </tr>
            </thead>
            <tbody>
              {products.slice(0, 20).map((p: any, i: number) => (
                <tr key={i}>
                  <td className="truncate">{p.product_name}</td>
                  <td>{p.views?.toLocaleString()}</td>
                  <td>{p.cart_adds?.toLocaleString()}</td>
                  <td>{p.purchases?.toLocaleString()}</td>
                  <td>${p.revenue?.toFixed(2)}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {conversionFunnel.length > 0 && (
        <div className="table-card">
          <h3>Conversion Funnel</h3>
          <div className="table-wrapper">
            <table className="data-table">
              <thead>
                <tr>
                  <th>Step</th>
                  <th>Users</th>
                  <th>Conversion %</th>
                </tr>
              </thead>
              <tbody>
                {conversionFunnel.map((c: any, i: number) => (
                  <tr key={i}>
                    <td>{c.step}</td>
                    <td>{c.users?.toLocaleString()}</td>
                    <td>{Math.round(c.conversion_rate || 0)}%</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}
    </div>
  );
}

function EventsTab({ events }: any) {
  return (
    <div className="table-card">
      <h3>Last Events (All)</h3>
      <div className="table-wrapper">
        <table className="data-table">
          <thead>
            <tr>
              <th>Timestamp</th>
              <th>Event Type</th>
              <th>Page URL</th>
              <th>Session ID</th>
            </tr>
          </thead>
          <tbody>
            {events.length === 0 ? (
              <tr>
                <td colSpan={4} className="muted">
                  No events yet.
                </td>
              </tr>
            ) : (
              events.map((e: any, i: number) => (
                <tr key={i}>
                  <td>{formatToJordanTime(e.timestamp)}</td>
                  <td className="mono">{e.event_type}</td>
                  <td className="mono truncate">{e.page_url}</td>
                  <td className="mono">{e.session_id}</td>
                </tr>
              ))
            )}
          </tbody>
        </table>
      </div>
    </div>
  );
}

import React, { useMemo, useState } from "react";
import { NavLink, useNavigate } from "react-router-dom";
import "./Setup.scss";
import { useAuth } from "../components/AuthContext";
import { trackEvent } from "../services/tracking";
import { logout } from "../services/auth";

type CommonEvent = { event_type: string; title: string; desc: string };

function uuidLike(): string {
  return (
    "trk_" + Math.random().toString(16).slice(2) + "_" + Date.now().toString(16)
  );
}

function CodeBlock({ label, code }: { label: string; code: string }) {
  const [copied, setCopied] = useState(false);

  async function copy() {
    try {
      await navigator.clipboard.writeText(code);
      setCopied(true);
      setTimeout(() => setCopied(false), 1200);
    } catch {}
  }

  return (
    <div className="code-block">
      <div className="code-label">{label}</div>
      <pre className="code-inner">
        <code>{code}</code>
      </pre>
      <button className="copy-btn" onClick={copy} type="button">
        {copied ? "Copied" : "Copy"}
      </button>
    </div>
  );
}

export default function Setup() {
  const { me } = useAuth();
  const [loading, setLoading] = useState(false);
  const [notice, setNotice] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const base = useMemo(() => {
    const pageUrl = window.location.origin + "/setup";
    const trackingId = uuidLike();
    return {
      tracking_id: trackingId,
      page_url: pageUrl,
      session_id: me?.id ? `sess_${me.id}` : undefined,
      api_key: `df_live_sk_${trackingId.slice(-18)}${Math.random().toString(16).slice(2, 10)}`,
    };
  }, [me?.id]);

  const navigate = useNavigate();

  const commonEvents: CommonEvent[] = useMemo(
    () => [
      {
        event_type: "page_view",
        title: "page_view",
        desc: "When users navigate to different pages",
      },
      {
        event_type: "button_click",
        title: "button_click",
        desc: "Track clicks on important buttons or CTAs",
      },
      {
        event_type: "form_submit",
        title: "form_submit",
        desc: "When users submit forms or complete actions",
      },
      {
        event_type: "user_signup",
        title: "user_signup",
        desc: "New user registration events",
      },
      {
        event_type: "purchase",
        title: "purchase",
        desc: "E-commerce transactions and purchases",
      },
      {
        event_type: "feature_used",
        title: "feature_used",
        desc: "Track usage of specific app features",
      },
      {
        event_type: "error_occurred",
        title: "error_occurred",
        desc: "Application errors and exceptions",
      },
      {
        event_type: "search_performed",
        title: "search_performed",
        desc: "When users search within your app",
      },
    ],
    [],
  );

  async function sendTestEvent() {
    setLoading(true);
    setNotice(null);
    setError(null);
    try {
      const res = await trackEvent({
        tracking_id: base.tracking_id,
        page_url: base.page_url,
        event_type: "test_event",
        session_id: base.session_id,
      });
      setNotice(res?.message || "Test event sent successfully.");
    } catch (ex: any) {
      setError(ex?.response?.data?.message || "Failed to send test event.");
    } finally {
      setLoading(false);
    }
  }
  
  const onLogout = async () => {
    await logout();
    navigate('/login');
  };

//   const sdkSnippet = `<script 
//   async 
// src="https://cdn.jsdelivr.net/gh/salahqr/Real-Time-Distributed-Analytics-Dashboard-Project@main/tracker/index.js"
//   data-endpoint="http://localhost:8080/receive_data"
//   data-tracking-id="${me?.id}"
//   data-batch-size="10"
//   data-interval="7000"
//   data-debug="true">
// </script>`;


// في Setup.tsx
const sdkSnippet = `<script 
  async 
  src="https://cdn.jsdelivr.net/gh/salahqr/Real-Time-Distributed-Analytics-Dashboard-Project@main/tracker/index.js"
  data-endpoint="http://localhost:8080/receive_data"
  data-tracking-id="${me?.id}"           
  data-user-id="${me?.id}"              
  data-batch-size="10"
  data-interval="7000"
  data-debug="true">
</script>`;

  return (
    <div className="integration-setup-container">
      <header className="main-header">
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
            <a href="#" className="logout" onClick={(e) => { e.preventDefault(); onLogout(); }}>
            <i className="fas fa-sign-out-alt"></i> Logout
          </a>
          {/* <a href="#" onClick={(e) => e.preventDefault()}>
            Documentation
          </a> */}
          {/* <a href="#" onClick={(e) => e.preventDefault()}>
            Support
          </a> */}
        </nav>
      </header>

      <main className="setup-content-wrapper">
        <h1 className="main-title">Integration Setup</h1>
        {/* <p className="subtitle">
          Add DataFlow Analytics to your application in minutes. Start
          collecting events and get real-time insights.
        </p> */}

        {/* <section className="project-info-card">
          <div className="info-row">
            <div className="info-item">
              <label>Project Name</label>
              <div className="value">
                {me?.company_name || "My Analytics Project"}
              </div>
            </div>
            <div className="info-item">
              <label>Project ID</label>
              <div className="value">
                {me?.id ? `proj_${me.id.slice(0, 10)}` : "proj_7x8k9m2n4p"}
              </div>
            </div>
            <div className="info-item">
              <label>Environment</label>
              <div className="value">Development</div>
            </div>
          </div>

          <div className="api-key-row">
            <div className="status-wrap">
              <label>Status</label>
              <div className="status-badge not-connected">Not Connected</div>
            </div>

            <div className="api-key-value">
              <label>API Key</label>
              <div className="key">{base.api_key}</div>
              <button
                type="button"
                className="mini-copy"
                onClick={() => navigator.clipboard.writeText(base.api_key)}
              >
                Copy
              </button>
            </div>
          </div>
        </section> */}

        <section className="setup-step">
          <div className="step-header">
            <h3>انسخ الكود التالي وضعه في ملف ال front end الخاص بك </h3>
          </div>
          <CodeBlock label="JAVASCRIPT" code={sdkSnippet} />
        </section>
      </main>
    </div>
  );
}

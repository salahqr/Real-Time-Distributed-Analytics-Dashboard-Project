import React from 'react';
import { useNavigate, Link } from 'react-router-dom';
import './Home.scss';

export default function Home() {
  const navigate = useNavigate();

  return (
    <div className="home-container">
      <header className="navbar">
        <div className="logo">
          <span>DataFlow Analytics</span>
        </div>
        <nav className="nav-links">
          <a href="#features">Why Choose Us?</a>
          <a href="#contact">Contact</a>
        </nav>
        <div className="auth-buttons">
          <button className="btn btn-login" onClick={() => navigate('/login')}>Login</button>
          <button className="btn btn-signup" onClick={() => navigate('/register')}>Sign Up</button>
        </div>
      </header>

      <section className="hero-section">
        <div className="hero-content">
          <h2>Real-Time Distributed Analytics</h2>
          <p className="hero-subtitle">
            Transform your data into actionable insights with our powerful real-time analytics
            dashboard. Monitor, analyze, and visualize your distributed systems with unprecedented
            clarity and speed.
          </p>
          <div className="hero-actions">
            <button className="btn btn-primary" onClick={() => navigate('/register')}>Get Started Free</button>
            <button className="btn btn-secondary" type="button">Watch Demo</button>
          </div>
        </div>
      </section>

      <section className="features-section" id="features">
        <div className="features-header">
          <h2 className="section-title">Why Choose DataFlow Analytics?</h2>
          <p className="section-subtitle">
            A platform built for speed, scale, and clarity in distributed environments.
          </p>
        </div>

        <div className="features-grid">
          <div className="feature-card">
            <i className="fas fa-chart-line icon-primary"></i>
            <h3 className="card-title">Real-Time Monitoring</h3>
            <p className="card-description">View data updates and system status instantly without latency.</p>
          </div>

          <div className="feature-card">
            <i className="fas fa-layer-group icon-primary"></i>
            <h3 className="card-title">Distributed Scale</h3>
            <p className="card-description">Handle massive event streams and scale effortlessly across multiple services.</p>
          </div>

          <div className="feature-card">
            <i className="fas fa-code icon-primary"></i>
            <h3 className="card-title">Custom Events Tracking</h3>
            <p className="card-description">Track page views, button clicks, and define custom events specific to your app's logic.</p>
          </div>

          <div className="feature-card">
            <i className="fas fa-database icon-primary"></i>
            <h3 className="card-title">Historical Data Analysis</h3>
            <p className="card-description">Access and analyze previous days' data for trend detection and long-term planning.</p>
          </div>

          <div className="feature-card">
            <i className="fas fa-plug icon-primary"></i>
            <h3 className="card-title">Simple Integration</h3>
            <p className="card-description">Get started quickly with easy-to-use integration code snippets and testing tools.</p>
          </div>

          <div className="feature-card">
            <i className="fas fa-lightbulb icon-primary"></i>
            <h3 className="card-title">Actionable Insights</h3>
            <p className="card-description">Translate complex data into clear, simple visualizations that drive business decisions.</p>
          </div>
        </div>
      </section>

      <section className="contact-section" id="contact">
        <div className="contact-content">
          <div className="contact-info">
            <h2 className="contact-title">Get in Touch</h2>
            <p className="contact-subtitle">
              Ready to revolutionize your data analytics? Our team of experts is here to help you get
              started with DataFlow Analytics and unlock the full potential of your data.
            </p>
            <div className="contact-details">
              <p><i className="fas fa-envelope"></i> contact@dataflow-analytics.com</p>
              <p><i className="fas fa-phone-alt"></i> +1 (555) 123-4567</p>
              <p><i className="fas fa-map-marker-alt"></i> 123 Analytics Street, Data City, DC 12345</p>
              <p><i className="fas fa-headset"></i> 24/7 Support Available</p>
            </div>
          </div>

          <div className="contact-form-container">
            <form className="contact-form" onSubmit={(e) => e.preventDefault()}>
              <input type="text" placeholder="Full Name" required />
              <input type="email" placeholder="Email Address" required />
              <input type="text" placeholder="Company" />
              <textarea
                placeholder="Message (Tell us about your analytics needs...)"
                rows={4}
                required
              />
              <button type="submit" className="btn btn-send-message">Send Message</button>
            </form>
          </div>
        </div>
      </section>

      <footer className="footer">
        <p className="copyright">
          © {new Date().getFullYear()} DataFlow Analytics. All rights reserved. | <Link to="/privacy">Privacy Policy</Link> |
          <Link to="/terms">Terms of Service</Link>
        </p>
      </footer>
    </div>
  );
}

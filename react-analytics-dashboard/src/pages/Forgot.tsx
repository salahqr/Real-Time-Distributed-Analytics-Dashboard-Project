import React, { useState } from 'react';
import { Link } from 'react-router-dom';
import './Forgot.scss';
import { forgotPassword } from '../services/auth';

export default function Forgot() {
  const [email, setEmail] = useState('');
  const [loading, setLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrorMessage(null);
    setSuccessMessage(null);
    if (!email.trim()) return setErrorMessage('Email is required.');

    setLoading(true);
    try {
      await forgotPassword({ email: email.trim() });
      setSuccessMessage('If the email exists, a reset link has been sent.');
    } catch (err: any) {
      const msg = err?.response?.data?.message || 'Failed to send reset link.';
      setErrorMessage(msg);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="auth-container">
      <div className="card forgot-card">
        <div className="logo"><i className="fas fa-lock"></i> Password Reset</div>
        <h2>?Forgot Your Password</h2>
        <p>Enter your email address and we'll send you a link to securely reset your password.</p>

        <form onSubmit={onSubmit}>
          {errorMessage && (
            <div className="alert error">
              <i className="fas fa-exclamation-triangle"></i> {errorMessage}
            </div>
          )}
          {successMessage && (
            <div className="alert success">
              <i className="fas fa-check-circle"></i> {successMessage}
            </div>
          )}

          <div className="form-group">
            <label htmlFor="email">Email Address</label>
            <input
              type="email"
              id="email"
              value={email}
              onChange={(e) => setEmail(e.target.value)}
              placeholder="e.g. user@company.com"
            />
          </div>

          <button type="submit" className="btn-primary" disabled={loading || !!successMessage || !email}>
            {loading ? 'Sending...' : 'Send Reset Link'}
          </button>
        </form>

        <div className="auth-link">
          <Link to="/login"><i className="fas fa-arrow-left"></i> Back to Login</Link>
        </div>
      </div>
    </div>
  );
}

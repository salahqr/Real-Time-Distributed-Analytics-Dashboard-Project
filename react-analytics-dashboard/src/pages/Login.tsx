import React, { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import "./Login.scss";
import { useAuth } from "../components/AuthContext";

export default function Login() {
  const { login } = useAuth();
  const navigate = useNavigate();

  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [loading, setLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [showPassword, setShowPassword] = useState(false);

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrorMessage(null);

    if (!email.trim()) return setErrorMessage("Email is required.");
    if (!password.trim()) return setErrorMessage("Password is required.");

    setLoading(true);
    try {
      await login(email.trim(), password);
      navigate("/dashboard");
    } catch (err: any) {
      const msg =
        err?.response?.data?.message ||
        "Login failed. Please check your credentials.";
      setErrorMessage(msg);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="login-container">
      <div className="login-card">
        <div className="form-panel">
          <div className="form-header">
            <div className="logo">
              <i className="fas fa-chart-line"></i> DataFlow
            </div>
            <h2>Welcome Back</h2>
            <p>Sign in to continue to your dashboard.</p>
          </div>

          <form onSubmit={onSubmit} className="login-form">
            {errorMessage && (
              <div className="alert error">
                <i className="fas fa-exclamation-triangle"></i> {errorMessage}
              </div>
            )}

            <div className="form-group">
              <label htmlFor="email">Email Address</label>
              <input
                type="email"
                id="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="e.g. admin@dataflow.com"
              />
            </div>

            <div className="form-group password-group">
              <label htmlFor="password">Password</label>
              <div className="input-wrapper">
                <input
                  type={showPassword ? "text" : "password"}
                  id="password"
                  value={password}
                  onChange={(e) => setPassword(e.target.value)}
                  placeholder="••••••••"
                />
                <span
                  className="toggle-password"
                  onClick={() => setShowPassword((prev) => !prev)}
                >
                  {showPassword ? (
                    <i className="fa-solid fa-eye"></i>
                  ) : (
                    <i className="fa-solid fa-eye-slash"></i>
                  )}
                </span>
              </div>
            </div>

            <div className="form-actions">
              <Link to="/forgot-password" className="forgot-password">
                ?Forgot Password
              </Link>
            </div>

            <button
              type="submit"
              className="btn-primary"
              disabled={loading || !email || !password}
            >
              {!loading ? "Sign In" : "Signing In..."}
            </button>
          </form>

          <div className="register-link">
            Don't have an account? <Link to="/register">Register Now</Link>
          </div>
        </div>

        <div className="illustration-panel">
          <img src="/login.png" alt="Login Illustration" />
        </div>
      </div>
    </div>
  );
}

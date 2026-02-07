import React, { useState } from "react";
import { Link, useNavigate } from "react-router-dom";
import "./Register.scss";
import { useAuth } from "../components/AuthContext";

export default function Register() {
  const { register } = useAuth();
  const navigate = useNavigate();

  const [name, setName] = useState("");
  const [companyName, setCompanyName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [passwordConfirmation, setPasswordConfirmation] = useState("");
  const [showPassword, setShowPassword] = useState(false);
  const [showPasswordConfirmation, setShowPasswordConfirmation] =
    useState(false);

  const [loading, setLoading] = useState(false);
  const [errorMessage, setErrorMessage] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  const onSubmit = async (e: React.FormEvent) => {
    e.preventDefault();
    setErrorMessage(null);
    setSuccessMessage(null);

    if (!name.trim()) return setErrorMessage("Name is required.");
    if (!email.trim()) return setErrorMessage("Email is required.");
    if (!password) return setErrorMessage("Password is required.");
    if (password.length < 6)
      return setErrorMessage("Password must be at least 6 characters.");
    if (password !== passwordConfirmation)
      return setErrorMessage("Passwords do not match.");

    setLoading(true);
    try {
      await register(
        name.trim(),
        email.trim(),
        password,
        companyName.trim() || undefined,
      );
      setSuccessMessage("Account created. Signing you in…");
      setTimeout(() => navigate("/setup"), 600);
    } catch (err: any) {
      const msg = err?.response?.data?.message || "Registration failed.";
      setErrorMessage(msg);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="register-container">
      <div className="register-card">
        <div className="form-panel">
          <div className="form-header">
            <div className="logo">
              <i className="fas fa-chart-line"></i> DataFlow
            </div>
            <h2>Join DataFlow</h2>
            <p>
              Create your account to start transforming your data into insights.
            </p>
          </div>

          <form onSubmit={onSubmit} className="register-form">
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
              <label htmlFor="name">Full Name</label>
              <input
                type="text"
                id="name"
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Your Full Name"
              />
            </div>

            <div className="form-group">
              <label htmlFor="companyName">Company Name</label>
              <input
                type="text"
                id="companyName"
                value={companyName}
                onChange={(e) => setCompanyName(e.target.value)}
                placeholder="Your Company Name"
              />
            </div>

            <div className="form-group">
              <label htmlFor="email">Email Address</label>
              <input
                type="email"
                id="email"
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="e.g. user@dataflow.com"
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

            <div className="form-group password-group">
              <label htmlFor="password_confirmation">Confirm Password</label>
              <div className="input-wrapper">
                <input
                  type={showPasswordConfirmation ? "text" : "password"}
                  id="password_confirmation"
                  value={passwordConfirmation}
                  onChange={(e) => setPasswordConfirmation(e.target.value)}
                  placeholder="••••••••"
                />
                <span
                  className="toggle-password"
                  onClick={() => setShowPasswordConfirmation((prev) => !prev)}
                >
                  {showPasswordConfirmation ? (
                    <i className="fa-solid fa-eye"></i>
                  ) : (
                    <i className="fa-solid fa-eye-slash"></i>
                  )}
                </span>
              </div>
            </div>

            <button
              type="submit"
              className="btn-primary"
              disabled={
                loading || !name || !email || !password || !passwordConfirmation
              }
            >
              {loading ? "Creating..." : "Register Account"}
            </button>
          </form>

          <div className="login-link">
            Already have an account? <Link to="/login">Sign In</Link>
          </div>
        </div>

        <div className="illustration-panel">
          <img src="/login.png" alt="Register Illustration" />
        </div>
      </div>
    </div>
  );
}

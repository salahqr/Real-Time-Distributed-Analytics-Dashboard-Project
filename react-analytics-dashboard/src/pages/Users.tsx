import React, { useEffect, useMemo, useState } from 'react';
import { Link, useNavigate } from 'react-router-dom';
import './Users.scss';
import { useAuth } from '../components/AuthContext';
import { ApiUser, listUsers } from '../services/users';

type UserVM = {
  id: string;
  name: string;
  email: string;
  companyName?: string | null;
  signupDate?: string;
};

function normalizeUser(u: ApiUser): UserVM {
  return {
    id: String(u.id),
    name: u.name || u.email || String(u.id),
    email: u.email || '—',
    companyName: u.company_name ?? null,
    signupDate: u.created_at,
  };
}

export default function Users() {
  const { logout } = useAuth();
  const navigate = useNavigate();

  const [users, setUsers] = useState<UserVM[]>([]);
  const [searchTerm, setSearchTerm] = useState('');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    (async () => {
      setLoading(true);
      setError(null);
      try {
        const res = await listUsers();
        setUsers((res || []).map(normalizeUser));
      } catch (e: any) {
        setError(e?.response?.data?.message || 'Failed to load users.');
        setUsers([]);
      } finally {
        setLoading(false);
      }
    })();
  }, []);

  const filteredUsers = useMemo(() => {
    const term = searchTerm.toLowerCase();
    return users.filter((u) => u.name.toLowerCase().includes(term) || u.email.toLowerCase().includes(term));
  }, [users, searchTerm]);

  const viewUserDetail = (userId: string) => {
    navigate(`/dashboard?user=${encodeURIComponent(userId)}`);
  };

  const onLogout = async () => {
    await logout();
    navigate('/login');
  };

  return (
    <div className="dashboard-layout">
      <div className="sidebar">
        <div className="logo">DataFlow</div>
        <nav className="nav-links">
          <Link to="/dashboard"><i className="fas fa-chart-line"></i> Dashboard</Link>
          <Link to="/users" className="active"><i className="fas fa-users"></i> Users</Link>
          <a href="#" onClick={(e) => e.preventDefault()}><i className="fas fa-stream"></i> Events Stream</a>
          <a href="#" onClick={(e) => e.preventDefault()}><i className="fas fa-cog"></i> Settings</a>
          <a href="#" className="logout" onClick={(e) => { e.preventDefault(); onLogout(); }}>
            <i className="fas fa-sign-out-alt"></i> Logout
          </a>
        </nav>
      </div>

      <div className="main-content">
        <header className="header">
          <h1>User Profiles ({filteredUsers.length} Active)</h1>
          <div className="search-bar">
            <input
              type="text"
              placeholder="Search by name or email..."
              value={searchTerm}
              onChange={(e) => setSearchTerm(e.target.value)}
              className="search-input"
            />
            <i className="fas fa-search search-icon"></i>
          </div>
        </header>

        {error && (
          <div style={{ padding: 12, margin: '0 0 12px', border: '1px solid rgba(255,255,255,0.12)', borderRadius: 12 }}>
            {error}
          </div>
        )}

        <div className="users-panel">
          <table className="users-table">
            <thead>
              <tr>
                <th>User Name</th>
                <th>Company</th>
                <th>Sign Up Date</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {loading ? (
                <tr><td colSpan={4} style={{ padding: 16 }}>Loading...</td></tr>
              ) : (
                filteredUsers.map((user) => (
                  <tr key={user.id}>
                    <td className="user-info">
                      <div className="avatar">{user.name?.charAt(0)?.toUpperCase()}</div>
                      <div>
                        <span className="name">{user.name}</span>
                        <span className="email">{user.email}</span>
                      </div>
                    </td>
                    <td>{user.companyName || '—'}</td>
                    <td>{user.signupDate ? new Date(user.signupDate).toLocaleDateString() : '—'}</td>
                    <td>
                      <button className="btn btn-view-detail" onClick={() => viewUserDetail(user.id)}>
                        View Profile <i className="fas fa-arrow-right"></i>
                      </button>
                    </td>
                  </tr>
                ))
              )}
            </tbody>
          </table>

          {!loading && filteredUsers.length === 0 && (
            <div className="no-results">
              <i className="fas fa-frown"></i>
              <p>No users found matching "{searchTerm}".</p>
            </div>
          )}
        </div>
      </div>
    </div>
  );
}

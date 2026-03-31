import React, { useState, useEffect, useCallback } from 'react';
import './App.css';
import SeismicMap from './SeismicMap';

const API_URL = process.env.REACT_APP_API_URL || 'http://localhost/api';
const WS_URL = process.env.REACT_APP_WS_URL || 'ws://localhost/ws';
const PAGE_SIZE = 20;

// ── helpers ──────────────────────────────────────────────
const getRowClass = (t) => {
  if (t === 'Nuclear-like event') return 'nuclear-like-event';
  if (t === 'Conventional explosion') return 'conventional-explosion';
  return 'earthquake';
};

const getRiskBadge = (t) => {
  if (t === 'Nuclear-like event') return <span className="badge badge-nuclear">🔴 CRITICAL</span>;
  if (t === 'Conventional explosion') return <span className="badge badge-explosion">🟡 HIGH</span>;
  return <span className="badge badge-earthquake">🟢 LOW</span>;
};

// ── Login page ────────────────────────────────────────────
const LoginPage = ({ onLogin }) => {
  const [user, setUser] = useState('');
  const [pass, setPass] = useState('');
  const [err, setErr] = useState('');

  const handleSubmit = (e) => {
    e.preventDefault();
    if (user === 'analyst' && pass === 'echo2026') {
      onLogin(user);
    } else {
      setErr('Invalid credentials. Access denied.');
    }
  };

  return (
    <div className="login-page">
      <div className="login-box">
        <div className="login-logo">⚡</div>
        <h1>E.C.H.O.</h1>
        <p className="login-subtitle">Strategic Monitor Access</p>
        <p className="login-warning">AUTHORIZED PERSONNEL ONLY</p>
        <form onSubmit={handleSubmit}>
          <input placeholder="Username or Employee ID" value={user}
            onChange={e => setUser(e.target.value)} required />
          <input type="password" placeholder="••••••••" value={pass}
            onChange={e => setPass(e.target.value)} required />
          {err && <p className="login-error">{err}</p>}
          <button type="submit" className="login-btn">🔒 SECURE LOGIN</button>
        </form>
        <p className="login-footer">🔐 SSL/TLS Encrypted Connection</p>
      </div>
    </div>
  );
};

// ── Nuclear modal ─────────────────────────────────────────
const NuclearModal = ({ alerts, onAcknowledge }) => {
  if (alerts.length === 0) return null;
  const latest = alerts[0];
  return (
    <div className="modal-overlay">
      <div className="modal-nuclear">
        <div className="modal-radiation">☢️</div>
        <h2>CRITICAL ALERT: NUCLEAR-LIKE ANOMALY DETECTED</h2>
        <p className="modal-detail">{latest}</p>
        <p className="modal-siren">📢 Audio Siren Active</p>
        <div className="modal-severity-bar">
          <div className="modal-severity-fill" />
        </div>
        <div className="modal-actions">
          <button className="btn-acknowledge" onClick={onAcknowledge}>
            ✅ Acknowledge & Mute Siren
          </button>
          <button className="btn-emergency">🚨 INITIATE EMERGENCY PROTOCOL</button>
        </div>
      </div>
    </div>
  );
};

// ── Export modal ──────────────────────────────────────────
const ExportModal = ({ onClose, onExportCSV }) => (
  <div className="modal-overlay">
    <div className="modal-export">
      <h2>📤 Export Report to External Command</h2>
      <div className="export-options">
        <label><input type="checkbox" defaultChecked /> Include Map Snippet</label>
        <label><input type="checkbox" defaultChecked /> Include Statistics Summary</label>
        <label><input type="checkbox" defaultChecked /> Include Raw Data Logs</label>
      </div>
      <div className="export-meta">
        <label>Classification Marking
          <select defaultValue="unclassified">
            <option value="unclassified">Unclassified</option>
            <option value="confidential">Confidential</option>
            <option value="secret">Secret</option>
          </select>
        </label>
        <label>External Commander / Department
          <input defaultValue="NATO Central Command" />
        </label>
      </div>
      <p className="export-note">Document will be digitally signed and encrypted.</p>
      <div className="modal-actions">
        <button className="btn-cancel" onClick={onClose}>Cancel</button>
        <button className="btn-export-csv" onClick={() => { onExportCSV(); onClose(); }}>
          ⬇ Download CSV
        </button>
      </div>
    </div>
  </div>
);

// ── Main Dashboard ────────────────────────────────────────
const Dashboard = ({ username, onLogout }) => {
  const [activeTab, setActiveTab] = useState('live');
  const [liveEvents, setLiveEvents] = useState([]);
  const [nuclearAlerts, setNuclearAlerts] = useState([]);
  const [showExport, setShowExport] = useState(false);
  const [connectionStatus, setConnectionStatus] = useState('Connecting...');
  const [lastUpdate, setLastUpdate] = useState(null);

  // History / filters
  const [filterSensor, setFilterSensor] = useState('');
  const [filterType, setFilterType] = useState('');
  const [filterFrom, setFilterFrom] = useState('');
  const [filterTo, setFilterTo] = useState('');
  const [activeChips, setActiveChips] = useState([]);
  const [historyEvents, setHistoryEvents] = useState([]);
  const [historyCount, setHistoryCount] = useState(0);
  const [historyPage, setHistoryPage] = useState(0);

  // US-23: WebSocket with reconnect
  useEffect(() => {
    let ws;
    let reconnectTimer;
    const connect = () => {
      ws = new WebSocket(WS_URL);
      ws.onopen = () => setConnectionStatus('Connected');
      ws.onclose = () => {
        setConnectionStatus('Disconnected. Reconnecting...');
        reconnectTimer = setTimeout(connect, 3000);
      };
      ws.onmessage = (msg) => {
        try {
          const evt = JSON.parse(msg.data);
          setLiveEvents(prev => [evt, ...prev].slice(0, 100));
          setLastUpdate(new Date());
          // US-24: nuclear modal
          if (evt.eventType === 'Nuclear-like event') {
            const alertMsg = `Nuclear-like event at sensor ${evt.sensorId} [${evt.location?.latitude}, ${evt.location?.longitude}] — Severity: ${evt.severityScore}`;
            setNuclearAlerts(prev => [alertMsg, ...prev]);
            new Audio('/alert-sound.mp3').play().catch(() => {});
          }
        } catch (e) { console.error(e); }
      };
    };
    connect();
    return () => { clearTimeout(reconnectTimer); ws?.close(); };
  }, []);

  // US-26/28: fetch history
  const fetchHistory = useCallback(async (page = 0) => {
    const params = new URLSearchParams();
    if (filterSensor) params.append('sensor_id', filterSensor);
    if (filterType) params.append('event_type', filterType);
    if (filterFrom) params.append('from_date', new Date(filterFrom).toISOString());
    if (filterTo) params.append('to_date', new Date(filterTo).toISOString());
    params.append('limit', PAGE_SIZE);
    params.append('offset', page * PAGE_SIZE);
    const [d, c] = await Promise.all([
      fetch(`${API_URL}/events?${params}`).then(r => r.json()),
      fetch(`${API_URL}/events/count?${params}`).then(r => r.json()),
    ]);
    setHistoryEvents(d);
    setHistoryCount(c.count);
    setHistoryPage(page);
    // build active filter chips
    const chips = [];
    if (filterSensor) chips.push({ label: `Sensor: ${filterSensor}`, key: 'sensor' });
    if (filterType) chips.push({ label: `Type: ${filterType}`, key: 'type' });
    if (filterFrom) chips.push({ label: `From: ${filterFrom}`, key: 'from' });
    if (filterTo) chips.push({ label: `To: ${filterTo}`, key: 'to' });
    setActiveChips(chips);
  }, [filterSensor, filterType, filterFrom, filterTo]);

  useEffect(() => { fetchHistory(0); }, [fetchHistory]);

  const removeChip = (key) => {
    if (key === 'sensor') setFilterSensor('');
    if (key === 'type') setFilterType('');
    if (key === 'from') setFilterFrom('');
    if (key === 'to') setFilterTo('');
  };

  // US-31: CSV export
  const exportCSV = () => {
    const headers = ['event_id','sensor_id','event_type','dominant_hz','severity_score','latitude','longitude','timestamp'];
    const rows = historyEvents.map(e =>
      [e.event_id, e.sensor_id, e.event_type, e.dominant_hz, e.severity_score, e.latitude, e.longitude, e.timestamp]
    );
    const csv = [headers, ...rows].map(r => r.join(',')).join('\n');
    const blob = new Blob([csv], { type: 'text/csv' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `echo_report_${Date.now()}.csv`;
    a.click();
  };

  const isOnline = connectionStatus === 'Connected';
  const totalPages = Math.max(1, Math.ceil(historyCount / PAGE_SIZE));

  return (
    <div className="dashboard-container">
      {/* US-24: Nuclear modal */}
      <NuclearModal alerts={nuclearAlerts} onAcknowledge={() => setNuclearAlerts([])} />
      {showExport && <ExportModal onClose={() => setShowExport(false)} onExportCSV={exportCSV} />}

      {/* Header */}
      <header>
        <div className="header-left">
          <span className="header-logo">⚡</span>
          <h1>E.C.H.O. Command Dashboard</h1>
        </div>
        <div className="header-right">
          <span className={`status-indicator ${isOnline ? 'online' : 'offline'}`}>
            {isOnline ? '● LIVE' : '○ OFFLINE'}
          </span>
          {lastUpdate && (
            <span className="last-update">Last update: {lastUpdate.toLocaleTimeString()}</span>
          )}
          <span className="username">👤 {username}</span>
          <button className="logout-btn" onClick={onLogout}>Logout</button>
        </div>
      </header>

      {/* US-27: Tabs for responsive navigation */}
      <nav className="tab-nav">
        {['live', 'map', 'history'].map(tab => (
          <button key={tab}
            className={`tab-btn ${activeTab === tab ? 'active' : ''}`}
            onClick={() => setActiveTab(tab)}>
            {tab === 'live' && '📡 Live Feed'}
            {tab === 'map' && '🌍 Sensor Map'}
            {tab === 'history' && '🔎 History Lab'}
          </button>
        ))}
      </nav>

      {/* TAB: Live Feed */}
      {activeTab === 'live' && (
        <div className="panel">
          <div className="panel-header">
            <h2>📡 Classified Live Stream</h2>
            <span className={`ws-badge ${isOnline ? 'ws-on' : 'ws-off'}`}>
              WebSocket: {isOnline ? 'Connected (Live)' : 'Disconnected'}
            </span>
          </div>
          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>Timestamp</th><th>Sensor ID</th><th>Event Type</th>
                  <th>Freq (Hz)</th><th>Severity</th><th>Risk Level</th>
                </tr>
              </thead>
              <tbody>
                {liveEvents.length === 0 && (
                  <tr><td colSpan={6} className="empty-row">Waiting for events...</td></tr>
                )}
                {liveEvents.map((evt, i) => (
                  <tr key={i} className={getRowClass(evt.eventType)}>
                    <td>{new Date(evt.timestamp).toLocaleTimeString()} {i === 0 && <span className="new-badge">NEW</span>}</td>
                    <td>{evt.sensorId}</td>
                    <td>{evt.eventType}</td>
                    <td>{evt.dominantFrequencyHz?.toFixed(2)}</td>
                    <td>{evt.severityScore}</td>
                    <td>{getRiskBadge(evt.eventType)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>
        </div>
      )}

      {/* TAB: Map */}
      {activeTab === 'map' && (
        <div className="panel">
          <h2>🌍 Global Sensor Network</h2>
          <div className="map-legend">
            <span className="legend-item legend-earthquake">● Earthquake</span>
            <span className="legend-item legend-explosion">● Conventional Explosion</span>
            <span className="legend-item legend-nuclear">● Nuclear-like</span>
          </div>
          <SeismicMap events={liveEvents} />
        </div>
      )}

      {/* TAB: History */}
      {activeTab === 'history' && (
        <div className="panel">
          <div className="panel-header">
            <h2>🔎 Seismic History Lab</h2>
            <button className="btn-export-open" onClick={() => setShowExport(true)}>
              📤 Export Report
            </button>
          </div>

          {/* Filter panel */}
          <div className="filter-panel">
            <select value={filterSensor} onChange={e => setFilterSensor(e.target.value)}>
              <option value="">Select Sensor ID</option>
              {[...new Set(historyEvents.map(e => e.sensor_id))].map(s => (
                <option key={s} value={s}>{s}</option>
              ))}
            </select>
            <select value={filterType} onChange={e => setFilterType(e.target.value)}>
              <option value="">All Event Types</option>
              <option value="Earthquake">Earthquake</option>
              <option value="Conventional explosion">Conventional explosion</option>
              <option value="Nuclear-like event">Nuclear-like event</option>
            </select>
            <input type="datetime-local" value={filterFrom} onChange={e => setFilterFrom(e.target.value)} />
            <input type="datetime-local" value={filterTo} onChange={e => setFilterTo(e.target.value)} />
            <button className="btn-search" onClick={() => fetchHistory(0)}>Run Analysis</button>
          </div>

          {/* US-28: Active filter chips */}
          {activeChips.length > 0 && (
            <div className="chips-row">
              {activeChips.map(chip => (
                <span key={chip.key} className="chip">
                  {chip.label}
                  <button onClick={() => removeChip(chip.key)}>✕</button>
                </span>
              ))}
            </div>
          )}

          {/* Stats counters */}
          <div className="stats-row">
            <div className="stat-card">
              <span className="stat-value">{historyCount}</span>
              <span className="stat-label">Events Found</span>
            </div>
            <div className="stat-card">
              <span className="stat-value">
                {historyEvents.length > 0
                  ? (historyEvents.reduce((s, e) => s + e.dominant_hz, 0) / historyEvents.length).toFixed(2)
                  : '—'}
              </span>
              <span className="stat-label">Avg. Freq (Hz)</span>
            </div>
            <div className="stat-card">
              <span className="stat-value">
                {historyEvents.length > 0
                  ? Math.max(...historyEvents.map(e => e.severity_score)).toFixed(1)
                  : '—'}
              </span>
              <span className="stat-label">Peak Severity</span>
            </div>
          </div>

          {/* Table */}
          <div className="table-wrapper">
            <table>
              <thead>
                <tr>
                  <th>Timestamp</th><th>Sensor ID</th><th>Event Type</th>
                  <th>Freq (Hz)</th><th>Severity</th><th>Risk</th>
                </tr>
              </thead>
              <tbody>
                {historyEvents.length === 0 && (
                  <tr><td colSpan={6} className="empty-row">No events found.</td></tr>
                )}
                {historyEvents.map((evt, i) => (
                  <tr key={i} className={getRowClass(evt.event_type)}>
                    <td>{new Date(evt.timestamp).toLocaleTimeString()}</td>
                    <td>{evt.sensor_id}</td>
                    <td>{evt.event_type}</td>
                    <td>{evt.dominant_hz?.toFixed(2)}</td>
                    <td>{evt.severity_score}</td>
                    <td>{getRiskBadge(evt.event_type)}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          {/* Pagination */}
          <div className="pagination">
            <button onClick={() => fetchHistory(historyPage - 1)} disabled={historyPage === 0}>← Prev</button>
            <span>Page {historyPage + 1} of {totalPages}</span>
            <button onClick={() => fetchHistory(historyPage + 1)} disabled={historyPage + 1 >= totalPages}>Next →</button>
          </div>
        </div>
      )}
    </div>
  );
};

// ── App root with auth gate ───────────────────────────────
const App = () => {
  const [username, setUsername] = useState(sessionStorage.getItem('echo_user') || null);

  const handleLogin = (user) => {
    sessionStorage.setItem('echo_user', user);
    setUsername(user);
  };

  const handleLogout = () => {
    sessionStorage.removeItem('echo_user');
    setUsername(null);
  };

  if (!username) return <LoginPage onLogin={handleLogin} />;
  return <Dashboard username={username} onLogout={handleLogout} />;
};

export default App;

import React, { useState, useEffect } from 'react';
import './App.css'; // Assuming you have some basic styling
import SeismicMap from './SeismicMap';

const Dashboard = () => {
  const [liveEvents, setLiveEvents] = useState([]);
  const [criticalAlerts, setCriticalAlerts] = useState([]);
  const [connectionStatus, setConnectionStatus] = useState('Connecting...');

  useEffect(() => {
    // Utilize the environment variable passed from docker-compose.yml
    const wsUrl = process.env.REACT_APP_WS_URL || 'ws://localhost/ws';
    const ws = new WebSocket(wsUrl);

    ws.onopen = () => setConnectionStatus('Connected to E.C.H.O. Gateway');
    ws.onclose = () => setConnectionStatus('Disconnected. Reconnecting...');
    
    ws.onmessage = (message) => {
      try {
        const eventData = JSON.parse(message.data);
        
        // Prepend new events to the real-time feed
        setLiveEvents((prevEvents) => [eventData, ...prevEvents]);

        // Business Logic: Trigger critical alerts for Nuclear-like events
        if (eventData.eventType === 'Nuclear-like event') {
          triggerCriticalAlert(eventData);
        }
      } catch (error) {
        console.error("Failed to parse incoming seismic data:", error);
      }
    };

    return () => {
      ws.close();
    };
  }, []);

  const triggerCriticalAlert = (eventData) => {
    const alertMsg = `CRITICAL THREAT: Nuclear-like event detected at [${eventData.location.latitude}, ${eventData.location.longitude}]. Severity: ${eventData.severityScore}`;
    
    setCriticalAlerts((prev) => [alertMsg, ...prev]);
    
    // HTML5 Audio API for the required audio alert
    const audio = new Audio('/alert-sound.mp3'); 
    audio.play().catch(e => console.log("Audio play blocked by browser interaction rules"));
  };

  return (
    <div className="dashboard-container">
      <header>
        <h1>E.C.H.O. Command Dashboard</h1>
        <div className={`status-indicator ${connectionStatus === 'Connected to E.C.H.O. Gateway' ? 'online' : 'offline'}`}>
          {connectionStatus}
        </div>
      </header>

      {/* Critical Alerts Banner */}
      {criticalAlerts.length > 0 && (
        <section className="alerts-panel">
          <h2>⚠️ CRITICAL ALERTS ⚠️</h2>
          <ul>
            {criticalAlerts.map((alert, index) => (
              <li key={index} className="nuclear-alert">{alert}</li>
            ))}
          </ul>
        </section>
      )}

      <main className="dashboard-grid">
        {/* The Interactive Map */}
        <section className="map-view">
          <h2>Geographical Origin Map</h2>
          <div className="map-placeholder" style={{ padding: 0, overflow: 'hidden' }}>
            <SeismicMap events={liveEvents} />
          </div>
        </section>

        {/* Real-time Event Feed */}
        <section className="live-feed">
          <h2>Live Seismic Feed</h2>
          <table>
            <thead>
              <tr>
                <th>Timestamp</th>
                <th>Sensor ID</th>
                <th>Event Type</th>
                <th>Freq (Hz)</th>
                <th>Severity</th>
              </tr>
            </thead>
            <tbody>
              {liveEvents.slice(0, 50).map((evt) => (
                <tr key={evt.eventId} className={evt.eventType.replace(/\s+/g, '-').toLowerCase()}>
                  <td>{new Date(evt.timestamp).toLocaleTimeString()}</td>
                  <td>{evt.sensorId}</td>
                  <td>{evt.eventType}</td>
                  <td>{evt.dominantFrequencyHz.toFixed(2)}</td>
                  <td>{evt.severityScore}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      </main>
    </div>
  );
};

export default Dashboard;
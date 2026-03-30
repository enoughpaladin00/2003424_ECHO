import React, { useState, useEffect } from 'react';
import { MapContainer, TileLayer, Marker, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';
import L from 'leaflet';

// Fix per le icone di Leaflet in React
delete L.Icon.Default.prototype._getIconUrl;
L.Icon.Default.mergeOptions({
  iconRetinaUrl: require('leaflet/dist/images/marker-icon-2x.png'),
  iconUrl: require('leaflet/dist/images/marker-icon.png'),
  shadowUrl: require('leaflet/dist/images/marker-shadow.png'),
});

function App() {
  const [events, setEvents] = useState([]);
  const API_URL = process.env.REACT_APP_API_URL || 'http://localhost/api';

  const WS_BASE = process.env.REACT_APP_WS_URL || 'ws://localhost/ws';
  const WS_URL = `${WS_BASE}/events`;

  useEffect(() => {
    const fetchInitialData = async () => {
      try {
        const response = await fetch(`${API_URL}/events`);
        if (!response.ok) throw new Error("Backend unreachable");
        const data = await response.json();
        setEvents(data);
      } catch (err) {
        console.error("Fetch error:", err);
      }
    };
    fetchInitialData();

    console.log("Attempting connection to:", WS_URL);
    const socket = new WebSocket(WS_URL);
    socket.onopen = () => console.log("WebSocket Connected");
    socket.onmessage = (msg) => {
      const newEvent = JSON.parse(msg.data);
      console.log("New Event Received:", newEvent);
      setEvents((prev) => {
        if(prev.find(e => e.event_id === newEvent.event_id)) return prev;
        return [newEvent, ...prev].slice(0, 50);
      });
    };

    socket.onerror = (err) => console.error("WebSocket Error:", err);

    return () => socket.close();
  }, [API_URL, WS_URL]);

  return (
    <div style={{ backgroundColor: '#111', color: '#0f0', minHeight: '100vh', fontFamily: 'monospace' }}>
      <header style={{ padding: '20px', textAlign: 'center', borderBottom: '2px solid #333' }}>
        <h2>COMMAND CENTER 2038 - STATUS: OPERATIVO</h2>
      </header>

      <div style={{ display: 'flex', padding: '20px', gap: '20px' }}>
        <div style={{ flex: '2', height: '70vh', border: '1px solid #333' }}>
          <MapContainer center={[45.0, 9.0]} zoom={4} style={{ height: '100%', width: '100%' }}>
            <TileLayer url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png" />
            {events.map((e) => (
              <Marker key={e.event_id} position={[e.latitude, e.longitude]}>
                <Popup>
                  <strong>ID: {e.sensor_id}</strong><br/>
                  Tipo: {e.event_type}
                </Popup>
              </Marker>
            ))}
          </MapContainer>
        </div>

        <div style={{ flex: '1', height: '70vh', overflowY: 'auto', border: '1px solid #333', padding: '15px' }}>
          <h3>REAL-TIME RELEVANCES</h3>
          <hr style={{ borderColor: '#333' }} />
          {events.length === 0 ? (
            <p>In ascolto di vibrazioni sismiche...</p>
          ) : (
            events.map((e, index) => (
              <div key={index} style={{ 
                marginBottom: '15px', 
                paddingBottom: '10px', 
                borderBottom: '1px dotted #333', 
                color: e.dominant_hz >= 8.0 ? '#ff0000' : (e.dominant_hz >= 3.0 ? '#ffaa00' : '#00ff00') 
              }}>
                <small>[{e.timestamp || 'RECENTS'}]</small><br/>
                <strong>{e.sensor_id}</strong> - {e.event_type}<br/>
                Freq: {e.dominant_hz} Hz
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
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

  useEffect(() => {
    const fetchData = async () => {
      try {
        const response = await fetch(`${API_URL}/events`); 
        const data = await response.json();
        if (Array.isArray(data)) {
          setEvents(data);
        }
      } catch (error) {
        console.error("Errore Dashboard:", error);
      }
    };
    fetchData(); 
    const interval = setInterval(fetchData, 2000); 
    return () => clearInterval(interval);
  }, [API_URL]);

  return (
    <div style={{ backgroundColor: '#111', color: '#0f0', minHeight: '100vh', fontFamily: 'monospace' }}>
      <header style={{ padding: '20px', textAlign: 'center', borderBottom: '2px solid #333' }}>
        <h2>COMMAND CENTER 2038 - STATUS: OPERATIVO</h2>
      </header>

      <div style={{ display: 'flex', padding: '20px', gap: '20px' }}>
        <div style={{ flex: '2', height: '70vh', border: '1px solid #333' }}>
          <MapContainer center={[45.0, 9.0]} zoom={4} style={{ height: '100%', width: '100%' }}>
            <TileLayer url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png" />
            {events.map((e, index) => (
              <Marker key={index} position={[e.lat, e.lng]}>
                <Popup>
                  <strong>ID Sensore: {e.sensor_id}</strong><br/>
                  Frequenza: {e.frequency} Hz<br/>
                  Tipo: {e.event_type}
                </Popup>
              </Marker>
            ))}
          </MapContainer>
        </div>

        <div style={{ flex: '1', height: '70vh', overflowY: 'auto', border: '1px solid #333', padding: '15px' }}>
          <h3>RILEVAMENTI REAL-TIME</h3>
          <hr style={{ borderColor: '#333' }} />
          {events.length === 0 ? (
            <p>In ascolto di vibrazioni sismiche...</p>
          ) : (
            events.map((e, index) => (
              <div key={index} style={{ 
                marginBottom: '15px', 
                paddingBottom: '10px', 
                borderBottom: '1px dotted #333', 
                color: e.frequency >= 8.0 ? '#ff0000' : (e.frequency >= 3.0 ? '#ffaa00' : '#00ff00') 
              }}>
                <small>[{e.timestamp || 'RECENTE'}]</small><br/>
                <strong>{e.sensor_id}</strong> - {e.event_type}<br/>
                Freq: {e.frequency} Hz
              </div>
            ))
          )}
        </div>
      </div>
    </div>
  );
}

export default App;
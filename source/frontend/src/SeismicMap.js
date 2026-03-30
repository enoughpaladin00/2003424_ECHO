import React, { useMemo } from 'react';
import { MapContainer, TileLayer, CircleMarker, Popup } from 'react-leaflet';
import 'leaflet/dist/leaflet.css';

const SeismicMap = ({ events }) => {
  const defaultCenter = [20.0, 0.0];
  const defaultZoom = 2;

  // 1. Group events to only show the LATEST event per sensor
  // Because your App.js prepends new events (putting them at index 0), 
  // the first time we see a sensorId, it is guaranteed to be the newest.
  const latestEvents = useMemo(() => {
    const sensorMap = {};
    events.forEach(event => {
      if (!sensorMap[event.sensorId]) {
        sensorMap[event.sensorId] = event;
      }
    });
    return Object.values(sensorMap);
  }, [events]);

  const getMarkerOptions = (eventType) => {
    switch (eventType) {
      case 'Nuclear-like event':
        return { color: '#ff7b72', fillColor: '#ff7b72', fillOpacity: 0.8, radius: 9 };
      case 'Conventional explosion':
        return { color: '#d2a8ff', fillColor: '#d2a8ff', fillOpacity: 0.7, radius: 7 };
      case 'Earthquake':
      default:
        return { color: '#a5d6ff', fillColor: '#a5d6ff', fillOpacity: 0.6, radius: 5 };
    }
  };

  return (
    <MapContainer 
      center={defaultCenter} 
      zoom={defaultZoom} 
      style={{ height: '100%', width: '100%', borderRadius: '4px' }}
      theme="dark"
    >
      <TileLayer
        url="https://{s}.basemaps.cartocdn.com/dark_all/{z}/{x}/{y}{r}.png"
        attribution='&copy; OpenStreetMap contributors &copy; CARTO'
      />
      
      {latestEvents.map((event) => (
        <CircleMarker
          // 2. Using the unique eventId as the key forces React to completely destroy 
          // and recreate the marker, which triggers the CSS radar-ping animation!
          key={event.eventId} 
          center={[event.location.latitude, event.location.longitude]}
          className="marker-ping"
          {...getMarkerOptions(event.eventType)}
        >
          <Popup>
            <div style={{ color: '#000', fontFamily: 'monospace' }}>
              <strong>{event.eventType}</strong><br />
              Sensor: {event.sensorId}<br />
              Freq: {event.dominantFrequencyHz.toFixed(2)} Hz<br />
              Severity: {event.severityScore}<br />
              Time: {new Date(event.timestamp).toLocaleTimeString()}
            </div>
          </Popup>
        </CircleMarker>
      ))}
    </MapContainer>
  );
};

export default SeismicMap;
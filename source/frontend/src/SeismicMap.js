import React, { useMemo } from 'react';
import { MapContainer, TileLayer, CircleMarker, Popup } from 'react-leaflet';

const SeismicMap = ({ events }) => {
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
      default:
        return { color: '#a5d6ff', fillColor: '#a5d6ff', fillOpacity: 0.6, radius: 5 };
    }
  };

  return (
    <MapContainer center={[20.0, 0.0]} zoom={2} style={{ height: '450px', width: '100%', borderRadius: '6px' }}>
      <TileLayer
        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png"
        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
      />
      {latestEvents.map((event) => (
        <CircleMarker
          key={event.sensorId}
          center={[event.location?.latitude ?? 0, event.location?.longitude ?? 0]}
          {...getMarkerOptions(event.eventType)}
        >
          <Popup>
            <div style={{ fontFamily: 'monospace', fontSize: '13px' }}>
              <strong>{event.eventType}</strong><br />
              Sensor: {event.sensorId}<br />
              Freq: {event.dominantFrequencyHz?.toFixed(2)} Hz<br />
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

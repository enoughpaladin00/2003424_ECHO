import React, { useState, useEffect } from 'react';

function App() {
  const [events, setEvents] = useState([]);
  const [alert, setAlert] = useState(false);

  useEffect(() => {
    // Si collega al simulatore fornito [cite: 46]
    const socket = new WebSocket('ws://localhost:8080/api/device/all/ws'); 
    socket.onmessage = (event) => {
      const data = JSON.parse(event.data);
      setEvents((prev) => [data, ...prev].slice(0, 10));
      // Allarme Nucleare se frequenza >= 8.0 Hz [cite: 92, 219]
      if (data.frequency >= 8.0) {
        setAlert(true);
        setTimeout(() => setAlert(false), 3000);
      }
    };
    return () => socket.close();
  }, []);

  return (
    <div style={{ backgroundColor: alert ? 'red' : '#1a1a1a', color: 'white', minHeight: '100vh', padding: '20px' }}>
      <h1>COMMAND CENTER 2038</h1>
      {alert && <h2>⚠️ ALLERTA NUCLEARE ⚠️</h2>}
      <div style={{ background: '#333', padding: '10px' }}>
        <h3>Feed Eventi</h3>
        {events.map((e, i) => (
          <div key={i}>ID: {e.sensor_id} | Freq: {e.frequency} Hz</div>
        ))}
      </div>
    </div>
  );
}
export default App;
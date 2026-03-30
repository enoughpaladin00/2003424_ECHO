import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional, List

import asyncpg
from fastapi import FastAPI, HTTPException, status, WebSocket, WebSocketDisconnect
from pydantic import BaseModel, Field, ConfigDict

# ==========================================
# CONFIGURATION
# ==========================================
DB_HOST = os.getenv("DB_HOST", "database")
DB_USER = os.getenv("DB_USER", "echo_admin")
DB_PASS = os.getenv("DB_PASS", "echodbpassword")
DB_NAME = os.getenv("DB_NAME", "echo_classified_events")
DB_PORT = int(os.getenv("DB_PORT", "5432"))

db_pool: asyncpg.Pool = None

# ==========================================
# WEBSOCKET CONNECTION MANAGER
# ==========================================
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        print(f"[ws] Dashboard client connected. Total: {len(self.active_connections)}")

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)
            print(f"[ws] Dashboard client disconnected. Total: {len(self.active_connections)}")

    async def broadcast_event(self, event_data: dict):
        # Broadcast the new seismic event to all connected dashboards
        for connection in self.active_connections.copy():
            try:
                await connection.send_json(event_data)
            except Exception as e:
                print(f"[ws] Failed to send to client: {e}")
                self.disconnect(connection)

manager = ConnectionManager()

# ==========================================
# DATABASE INIT
# ==========================================
CREATE_TABLE_SQL = """
CREATE TABLE IF NOT EXISTS seismic_events (
    id              SERIAL PRIMARY KEY,
    event_id        VARCHAR(64)   UNIQUE NOT NULL,
    sensor_id       VARCHAR(64)   NOT NULL,
    event_type      VARCHAR(64)   NOT NULL,
    dominant_hz     FLOAT         NOT NULL,
    latitude        FLOAT         NOT NULL,
    longitude       FLOAT         NOT NULL,
    magnitude       FLOAT,
    severity_score  FLOAT         NOT NULL,
    timestamp       TIMESTAMPTZ   NOT NULL,
    replica_id      VARCHAR(64),
    created_at      TIMESTAMPTZ   NOT NULL DEFAULT NOW()
);
"""

# ==========================================
# LIFESPAN
# ==========================================
@asynccontextmanager
async def lifespan(app: FastAPI):
    global db_pool
    for attempt in range(10):
        try:
            db_pool = await asyncpg.create_pool(
                host=DB_HOST, port=DB_PORT,
                user=DB_USER, password=DB_PASS,
                database=DB_NAME,
                min_size=2, max_size=10,
            )
            break
        except Exception as e:
            print(f"[db] Connection attempt {attempt + 1}/10 failed: {e}. Retrying in 2s...")
            await asyncio.sleep(2)
    else:
        raise RuntimeError("Could not connect to PostgreSQL after 10 attempts.")

    async with db_pool.acquire() as conn:
        await conn.execute(CREATE_TABLE_SQL)
        print("[db] Table ready.")

    yield

    await db_pool.close()
    print("[db] Pool closed.")

# ==========================================
# FASTAPI APP
# ==========================================
app = FastAPI(title="E.C.H.O. Persistence Layer", lifespan=lifespan)

# ==========================================
# PYDANTIC MODEL
# ==========================================
class Location(BaseModel):
    latitude: float
    longitude: float

class SeismicEventIn(BaseModel):
    model_config = ConfigDict(populate_by_name=True)

    event_id:       str                = Field(..., alias="eventId")
    sensor_id:      str                = Field(..., alias="sensorId")
    event_type:     str                = Field(..., alias="eventType")
    dominant_hz:    float              = Field(..., alias="dominantFrequencyHz")
    location:       Location
    timestamp:      datetime
    severity_score: float              = Field(..., alias="severityScore")
    
    magnitude:      Optional[float]    = Field(None, alias="_rawMagnitude")
    replica_id:     Optional[str]      = Field(None, alias="_replicaId")

# ==========================================
# ENDPOINTS
# ==========================================
INSERT_SQL = """
INSERT INTO seismic_events
    (event_id, sensor_id, event_type, dominant_hz, latitude, longitude, magnitude, severity_score, timestamp, replica_id)
VALUES
    ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
ON CONFLICT (event_id) DO NOTHING;
"""

@app.post("/events", status_code=status.HTTP_201_CREATED)
async def create_event(event: SeismicEventIn):
    async with db_pool.acquire() as conn:
        # asyncpg execute returns a status string like "INSERT 0 1" (success) or "INSERT 0 0" (conflict ignored)
        result = await conn.execute(
            INSERT_SQL,
            event.event_id,
            event.sensor_id,
            event.event_type,
            event.dominant_hz,
            event.location.latitude,
            event.location.longitude,
            event.magnitude,
            event.severity_score,
            event.timestamp,
            event.replica_id,
        )
    
    # Only broadcast to the dashboard if it's a completely new event, preventing UI duplicates from replicas
    if result == "INSERT 0 1":
        # model_dump with mode='json' perfectly converts dates to ISO 8601 strings for the websocket
        event_payload = event.model_dump(by_alias=True, mode='json')
        await manager.broadcast_event(event_payload)

    return {"status": "accepted"}

# Added both routes to ensure Nginx trailing slash proxy rules don't cause 404s
@app.websocket("/ws")
@app.websocket("/ws/")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            # Keep the connection open and listen for any client pings
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

@app.get("/health", status_code=status.HTTP_200_OK)
async def health():
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB unreachable: {e}")
    return {"status": "ok", "service": "persistence"}
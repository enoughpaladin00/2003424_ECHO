import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime, timedelta, timezone
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
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(64) UNIQUE NOT NULL,
    sensor_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    dominant_hz FLOAT NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    magnitude FLOAT,
    severity_score FLOAT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    replica_id VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- US-21: Index on timestamp for historical queries and time-series filtering
CREATE INDEX IF NOT EXISTS idx_seismic_events_timestamp
    ON seismic_events (timestamp DESC);

-- US-21: Index on sensor_id for sensor-based filtering (US-26/28)
CREATE INDEX IF NOT EXISTS idx_seismic_events_sensor_id
    ON seismic_events (sensor_id);

-- US-21: Index on event_type for type-based filtering (US-26/28)
CREATE INDEX IF NOT EXISTS idx_seismic_events_event_type
    ON seismic_events (event_type);

-- US-21: Composite index on lat/lon for geographic queries
CREATE INDEX IF NOT EXISTS idx_seismic_events_location
    ON seismic_events (latitude, longitude);


-- US-22: archive table for events older than 30 days
CREATE TABLE IF NOT EXISTS seismic_events_archive (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(64) UNIQUE NOT NULL,
    sensor_id VARCHAR(64) NOT NULL,
    event_type VARCHAR(64) NOT NULL,
    dominant_hz FLOAT NOT NULL,
    latitude FLOAT NOT NULL,
    longitude FLOAT NOT NULL,
    magnitude FLOAT,
    severity_score FLOAT NOT NULL,
    timestamp TIMESTAMPTZ NOT NULL,
    replica_id VARCHAR(64),
    created_at TIMESTAMPTZ NOT NULL,
    archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- US-30: Audit log for login and export events
CREATE TABLE IF NOT EXISTS audit_logs (
    id SERIAL PRIMARY KEY,
    action VARCHAR(64) NOT NULL,
    username VARCHAR(128) NOT NULL,
    detail TEXT,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
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
        asyncio.create_task(run_data_retention(), name="data-retention")  # ← add this


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
    
    magnitude:      Optional[float]    = Field(None, alias="rawMagnitude")
    replica_id:     Optional[str]      = Field(None, alias="replicaId")

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


# ==========================================
# QUERY ENDPOINTS (US-26, US-28)
# ==========================================

@app.get("/events")
async def get_events(
    sensor_id: Optional[str] = None,
    event_type: Optional[str] = None,
    from_date: Optional[datetime] = None,
    to_date: Optional[datetime] = None,
    limit: int = 100,
    offset: int = 0,
):
    """
    Returns historical seismic events with optional filters.
    Supports filtering by sensor_id, event_type, and date range.
    Used by the dashboard for historical inspection (US-26) and
    targeted analysis by region/type (US-28).
    """
    conditions = []
    params = []
    idx = 1

    if sensor_id:
        conditions.append(f"sensor_id = ${idx}")
        params.append(sensor_id)
        idx += 1

    if event_type:
        conditions.append(f"event_type = ${idx}")
        params.append(event_type)
        idx += 1

    if from_date:
        conditions.append(f"timestamp >= ${idx}")
        params.append(from_date)
        idx += 1

    if to_date:
        conditions.append(f"timestamp <= ${idx}")
        params.append(to_date)
        idx += 1

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""

    query = f"""
        SELECT event_id, sensor_id, event_type, dominant_hz,
               latitude, longitude, severity_score, timestamp, replica_id
        FROM seismic_events
        {where_clause}
        ORDER BY timestamp DESC
        LIMIT ${idx} OFFSET ${idx + 1}
    """
    params.extend([limit, offset])

    async with db_pool.acquire() as conn:
        rows = await conn.fetch(query, *params)

    return [dict(row) for row in rows]



@app.get("/events/count")
async def count_events(
    sensor_id: Optional[str] = None,
    event_type: Optional[str] = None,
):
    """
    Returns the total count of events matching the given filters.
    Used by the dashboard for pagination (US-26).
    """
    conditions = []
    params = []
    idx = 1

    if sensor_id:
        conditions.append(f"sensor_id = ${idx}")
        params.append(sensor_id)
        idx += 1

    if event_type:
        conditions.append(f"event_type = ${idx}")
        params.append(event_type)
        idx += 1

    where_clause = ("WHERE " + " AND ".join(conditions)) if conditions else ""
    query = f"SELECT COUNT(*) FROM seismic_events {where_clause}"

    async with db_pool.acquire() as conn:
        count = await conn.fetchval(query, *params)

    return {"count": count}


# ==========================================
# DATA RETENTION (US-22)
# ==========================================

async def run_data_retention() -> None:
    while True:
        now = datetime.now(timezone.utc)
        tomorrow = now + timedelta(days=1)
        next_midnight = tomorrow.replace(hour=0, minute=0, second=0, microsecond=0)
        wait_seconds = (next_midnight - now).total_seconds()
        print(f"[retention] Next run in {wait_seconds/3600:.1f} hours (at midnight UTC).")
        await asyncio.sleep(wait_seconds)

        try:
            async with db_pool.acquire() as conn:
                async with conn.transaction():
                    archived = await conn.execute("""
                        INSERT INTO seismic_events_archive
                            (event_id, sensor_id, event_type, dominant_hz,
                             latitude, longitude, magnitude, severity_score,
                             timestamp, replica_id, created_at)
                        SELECT
                            event_id, sensor_id, event_type, dominant_hz,
                            latitude, longitude, magnitude, severity_score,
                            timestamp, replica_id, created_at
                        FROM seismic_events
                        WHERE timestamp < NOW() - INTERVAL '30 days'
                        ON CONFLICT (event_id) DO NOTHING;
                    """)
                    deleted = await conn.execute("""
                        DELETE FROM seismic_events
                        WHERE timestamp < NOW() - INTERVAL '30 days';
                    """)
                    print(f"[retention] Archived: {archived} | Deleted from active: {deleted}")

        except Exception as e:
            print(f"[retention] Error during data retention run: {e}")





# ==========================================
# AUDIT LOG (US-30)
# ==========================================

class AuditLogIn(BaseModel):
    action:   str
    username: str
    detail:   Optional[str] = None

@app.post("/audit", status_code=status.HTTP_201_CREATED)
async def create_audit_log(entry: AuditLogIn):
    async with db_pool.acquire() as conn:
        await conn.execute(
            "INSERT INTO audit_logs (action, username, detail) VALUES ($1, $2, $3)",
            entry.action, entry.username, entry.detail,
        )
    return {"status": "logged"}

@app.get("/audit")
async def get_audit_logs(limit: int = 100, offset: int = 0):
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            "SELECT id, action, username, detail, created_at FROM audit_logs ORDER BY created_at DESC LIMIT $1 OFFSET $2",
            limit, offset,
        )
    return [dict(row) for row in rows]


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
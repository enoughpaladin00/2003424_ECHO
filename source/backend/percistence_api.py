import asyncio
import os
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import asyncpg
from fastapi import FastAPI, HTTPException, status
from fastapi.middleware.cors import CORSMiddleware
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
    print(f"[db] Connecting to {DB_HOST}...")
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

# Add CORS so the Dashboard (Frontend) can talk to this API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ==========================================
# PYDANTIC MODEL
# Accepts both camelCase (from processing engine) and snake_case
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
    
    # Optional debugging fields
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
@app.get("/api/events")
async def get_events(limit: int = 50):
    """
    Returns the most recent classified seismic events for the dashboard.
    """
    async with db_pool.acquire() as conn:
        rows = await conn.fetch(
            'SELECT event_id, sensor_id, event_type, dominant_hz, latitude, longitude, severity_score, timestamp '
            'FROM seismic_events ORDER BY timestamp DESC LIMIT $1',
            limit
        )
        # Convert record objects to a list of dictionaries for JSON serialization
        return [dict(row) for row in rows]

@app.post("/api/events", status_code=status.HTTP_201_CREATED)
async def create_event(event: SeismicEventIn):
    async with db_pool.acquire() as conn:
        await conn.execute(
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
    return {"status": "accepted"}

@app.get("/api/health", status_code=status.HTTP_200_OK)
async def health():
    """Healthcheck endpoint — also verifies DB connectivity."""
    try:
        async with db_pool.acquire() as conn:
            await conn.fetchval("SELECT 1")
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"DB unreachable: {e}")
    return {"status": "ok", "service": "persistence"}
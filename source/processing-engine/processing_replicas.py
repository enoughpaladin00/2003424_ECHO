# main.py
# E.C.H.O. Processing Replica
# Covers: FFT analysis, sliding window, event classification, severity score,
#         broker ingestion, SSE shutdown listener, persistence, FastAPI health endpoint

# ==========================================
# 1. IMPORTS
# ==========================================
import asyncio
import os
import sys
import time
import json
import uuid
import httpx
import websockets
import numpy as np
from collections import deque
from contextlib import asynccontextmanager
from fastapi import FastAPI
import uvicorn
from typing import Optional

# ==========================================
# 2. CONFIGURATION
# ==========================================

BROKER_WS_URL  = os.getenv("BROKER_WS_URL",  "ws://localhost:8000")
SIMULATOR_URL  = os.getenv("SIMULATOR_URL",   "http://localhost:8080")
DB_SERVICE_URL = os.getenv("DB_SERVICE_URL",  "http://persistence:8001")
WINDOW_SIZE    = int(os.getenv("WINDOW_SIZE", "100"))
FFT_STEP       = int(os.getenv("FFT_STEP",    "10"))
THRESHOLD      = float(os.getenv("THRESHOLD", "80.0"))
FS             = int(os.getenv("FS",          "20"))
REPLICA_ID     = os.getenv("REPLICA_ID",      "replica-?")

# ==========================================
# 3. CORE MATH LOGIC (FFT & Classification)
# ==========================================

# Exact enum values as defined in the E.C.H.O. event schema
EVENT_TYPE_EARTHQUAKE  = "Earthquake"
EVENT_TYPE_EXPLOSION   = "Conventional explosion"   # lowercase 'e' — matches schema enum
EVENT_TYPE_NUCLEAR     = "Nuclear-like event"
EVENT_TYPE_NONE        = "No significant event"


def compute_severity_score(max_magnitude: float, threshold: float) -> float:
    """
    Calculates a normalised severity score [0.0 – 100.0] based on signal amplitude.
    Score is proportional to how far the magnitude exceeds the noise threshold.
    Capped at 100 to keep values human-readable on the dashboard.

    Formula: score = min((magnitude / threshold) * 50, 100.0)
    A magnitude exactly at threshold → score 50 (moderate).
    A magnitude 2× threshold        → score 100 (maximum).
    """
    if threshold <= 0:
        return 0.0
    return round(min((max_magnitude / threshold) * 50.0, 100.0), 2)


def analyze_seismic_window(
    window,
    fs: int = 20,
    threshold: float = 80.0
) -> tuple[str, float, float, float]:
    """
    Analyses a seismic time-domain window using real FFT.

    Args:
        window:    array of seismic samples (mm/s)
        fs:        sampling frequency in Hz (must match simulator SAMPLING_RATE_HZ)
        threshold: minimum FFT magnitude to trigger classification (noise gate)

    Returns:
        (event_type, dominant_freq_hz, max_magnitude, severity_score)
        event_type is one of the EVENT_TYPE_* constants above.
    """
    window = np.asarray(window, dtype=float)
    window = window - np.mean(window)   # remove DC offset before FFT

    freqs      = np.fft.rfftfreq(len(window), d=1 / fs)
    magnitudes = np.abs(np.fft.rfft(window))
    magnitudes[0] = 0.0                 # zero DC bin as safety guard

    max_idx       = int(np.argmax(magnitudes))
    dominant_freq = float(freqs[max_idx])
    max_magnitude = float(magnitudes[max_idx])

    # Noise gate — skip classification if signal is too weak
    if max_magnitude < threshold:
        return EVENT_TYPE_NONE, dominant_freq, max_magnitude, 0.0

    # Classify by dominant frequency band (Rule Model §A)
    if 0.5 <= dominant_freq < 3.0:
        event_type = EVENT_TYPE_EARTHQUAKE
    elif 3.0 <= dominant_freq < 8.0:
        event_type = EVENT_TYPE_EXPLOSION
    elif dominant_freq >= 8.0:
        event_type = EVENT_TYPE_NUCLEAR
    else:
        event_type = "Unclassified Anomaly"

    severity = compute_severity_score(max_magnitude, threshold)
    return event_type, dominant_freq, max_magnitude, severity


# ==========================================
# 4. SLIDING WINDOW STATE
# ==========================================

class SeismicReplicaState:
    """
    Manages per-sensor sliding windows and runs FFT-based event detection
    on each incoming sample. One instance per processing replica.
    """

    def __init__(
        self,
        window_size: int = 100,
        fs: int = 20,
        threshold: float = 80.0,
        step: int = 10
    ):
        """
        Args:
            window_size: samples per analysis window (100 @ 20 Hz = 5 s)
            fs:          sampling frequency — must match simulator SAMPLING_RATE_HZ
            threshold:   minimum FFT magnitude to trigger an event (noise gate)
            step:        run FFT every N new samples (hop size) to avoid redundant
                         computation on heavily overlapping windows
        """
        self.window_size = window_size
        self.fs          = fs
        self.threshold   = threshold
        self.step        = step

        self.sensor_buffers:   dict[str, deque] = {}
        self._sample_counters: dict[str, int]   = {}

    def process_incoming_sample(
        self,
        sensor_id: str,
        value: float,
        timestamp: Optional[str] = None,
        location: Optional[dict] = None,
    ) -> Optional[dict]:
        """
        Call for every measurement received from the broker.

        Returns a fully schema-compliant detection dict if an event is found,
        otherwise None. The dict is ready to POST to the persistence layer.

        Schema fields returned:
            eventId, sensorId, timestamp, location,
            dominantFrequencyHz, eventType, severityScore
        """
        # Lazy-initialise buffer and counter for newly seen sensors
        if sensor_id not in self.sensor_buffers:
            self.sensor_buffers[sensor_id]   = deque(maxlen=self.window_size)
            self._sample_counters[sensor_id] = 0

        self.sensor_buffers[sensor_id].append(value)
        self._sample_counters[sensor_id] += 1

        # Wait for a full window before any analysis
        if len(self.sensor_buffers[sensor_id]) < self.window_size:
            return None

        # Run FFT only every `step` samples (sliding window with hop)
        if self._sample_counters[sensor_id] % self.step != 0:
            return None

        window_data = np.array(self.sensor_buffers[sensor_id])
        event_type, dominant_freq, magnitude, severity = analyze_seismic_window(
            window_data, fs=self.fs, threshold=self.threshold
        )

        if event_type == EVENT_TYPE_NONE:
            return None

        # Build the canonical E.C.H.O. event payload (camelCase — schema compliant)
        return {
            "eventId": str(uuid.uuid5(uuid.NAMESPACE_URL,f"{sensor_id}:{timestamp}:{event_type}")),  # unique UUID per detection
            "sensorId":            sensor_id,
            "timestamp":           timestamp or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "location":            location or {"latitude": 0.0, "longitude": 0.0},
            "dominantFrequencyHz": round(dominant_freq, 2),
            "eventType":           event_type,          # exact schema enum value
            "severityScore":       severity,
            # Internal metadata — not in public schema, useful for debugging
            "_replicaId":          REPLICA_ID,
            "_rawMagnitude":       round(magnitude, 2),
        }


# ==========================================
# 5. STATE INSTANCE
# ==========================================

state = SeismicReplicaState(
    window_size=WINDOW_SIZE,
    fs=FS,
    threshold=THRESHOLD,
    step=FFT_STEP
)

# ==========================================
# 6. ASYNC TASKS
# ==========================================

async def persist_event(event: dict) -> None:
    """
    Fire-and-forget POST to the persistence service.
    The persistence layer is responsible for idempotent insertion (US-20).
    The unique eventId UUID ensures deduplication across replicas.
    """
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{DB_SERVICE_URL}/events", json=event, timeout=5.0)
        except Exception as e:
            print(f"[persist] Failed to store event {event.get('eventId')}: {e}")


async def listen_to_broker() -> None:
    """
    Maintains a persistent WebSocket connection to the broker (US-5, US-6).
    Reconnects automatically on any transient failure.
    Expected message format from broker: { sensor_id, timestamp, value, [location] }
    """
    print(f"[broker] Connecting to {BROKER_WS_URL} ...")
    while True:
        try:
            async with websockets.connect(BROKER_WS_URL) as ws:
                print(f"[broker] Connected. Replica: {REPLICA_ID}")
                async for raw_msg in ws:
                    try:
                        msg = json.loads(raw_msg)
                    except json.JSONDecodeError:
                        print(f"[broker] Malformed JSON skipped: {raw_msg[:80]}")
                        continue

                    sensor_id = msg.get("sensor_id") or msg.get("sensorId")
                    value     = msg.get("value")
                    timestamp = msg.get("timestamp")
                    location  = msg.get("location")    # pass through if broker enriches it

                    if sensor_id is None or value is None:
                        print(f"[broker] Missing required fields, skipping: {msg}")
                        continue

                    detection = state.process_incoming_sample(
                        sensor_id=sensor_id,
                        value=float(value),
                        timestamp=timestamp,
                        location=location,
                    )
                    if detection:
                        print(
                            f"[detection] {detection['eventType']} | "
                            f"sensor={detection['sensorId']} | "
                            f"freq={detection['dominantFrequencyHz']} Hz | "
                            f"severity={detection['severityScore']}"
                        )
                        asyncio.create_task(persist_event(detection))

        except Exception as e:
            print(f"[broker] Connection lost: {e}. Retrying in 3s...")
            await asyncio.sleep(3)


async def listen_for_shutdown() -> None:
    """
    Listens to the simulator's SSE control stream (US-15).
    Self-terminates the replica upon receiving a SHUTDOWN command.

    SSE lines have the format:
         {"command": "SHUTDOWN"}
    The prefix is literally the 5 characters 'd','a','t','a',':'
    """
    url = f"{SIMULATOR_URL}/api/control"
    SSE_PREFIX = "data:"          # explicit string constant — avoids invisible char bugs

    while True:
        try:
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream("GET", url) as response:
                    async for line in response.aiter_lines():
                        if not line.startswith(SSE_PREFIX):
                            continue

                        payload_str = line[len(SSE_PREFIX):].strip()
                        if not payload_str:
                            continue

                        try:
                            payload = json.loads(payload_str)
                        except json.JSONDecodeError:
                            continue

                        if payload.get("command") == "SHUTDOWN":
                            print("[control] SHUTDOWN received. Terminating replica.")
                            os._exit(0)

        except Exception as e:
            print(f"[control] SSE stream lost: {e}. Retrying in 3s...")
            await asyncio.sleep(3)


# ==========================================
# 7. FASTAPI APP
# ==========================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    asyncio.create_task(listen_to_broker(),    name="broker-listener")
    asyncio.create_task(listen_for_shutdown(), name="shutdown-listener")
    yield

app = FastAPI(title="E.C.H.O. Processing Replica", lifespan=lifespan)


@app.get("/health")
def health():
    """Health endpoint for broker/gateway active health-checks (US-16)."""
    return {
        "status":          "ok",
        "replica_id":      REPLICA_ID,
        "sensors_tracked": list(state.sensor_buffers.keys()),
        "buffer_sizes":    {k: len(v) for k, v in state.sensor_buffers.items()},
    }


if __name__ == "__main__":
    uvicorn.run("processing_replicas:app", host="0.0.0.0", port=8002, reload=False)

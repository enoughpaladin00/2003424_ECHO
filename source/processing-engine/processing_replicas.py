# ==========================================
# E.C.H.O. Processing Replica
# Covers: FFT analysis, sliding window, event classification, severity score,
#         broker ingestion, SSE shutdown listener, persistence, FastAPI health endpoint
# ==========================================

import asyncio
import socket
import os
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
import sys
sys.stdout.reconfigure(line_buffering=True) 

# ==========================================
# 2. CONFIGURATION
# ==========================================

BROKER_WS_URL  = os.getenv("BROKER_WS_URL",  "ws://ingestion-broker:8000")
SIMULATOR_URL  = os.getenv("SIMULATOR_URL",   "http://seismic-simulator:8080")
DB_SERVICE_URL = os.getenv("DB_SERVICE_URL",  "http://backend-api:3000")

WINDOW_SIZE    = int(os.getenv("WINDOW_SIZE", "100"))
FFT_STEP       = int(os.getenv("FFT_STEP",    "10"))
THRESHOLD      = float(os.getenv("THRESHOLD", "40.0"))
FS             = int(os.getenv("FS",          "20"))
REPLICA_ID     = os.getenv("REPLICA_ID", socket.gethostname())

# ==========================================
# 3. CORE MATH LOGIC (FFT & Classification)
# ==========================================

EVENT_TYPE_EARTHQUAKE = "Earthquake"
EVENT_TYPE_EXPLOSION  = "Conventional explosion"
EVENT_TYPE_NUCLEAR    = "Nuclear-like event"
EVENT_TYPE_NONE       = "No significant event"


def compute_severity_score(max_magnitude: float, threshold: float) -> float:
    if threshold <= 0:
        return 0.0
    return round(min((max_magnitude / threshold) * 50.0, 100.0), 2)


def analyze_seismic_window(
    window,
    fs: int = 20,
    threshold: float = 40.0
) -> tuple[str, float, float, float]:
    window = np.asarray(window, dtype=float)

    # Detrend: rimuove offset DC
    window = window - np.mean(window)
    # Hanning window: previene spectral leakage
    window = window * np.hanning(len(window))

    freqs      = np.fft.rfftfreq(len(window), d=1 / fs)
    magnitudes = np.abs(np.fft.rfft(window))

    # Azzera DC e rumore a bassissima frequenza (< 0.2 Hz)
    magnitudes[freqs < 0.2] = 0.0

    max_idx       = int(np.argmax(magnitudes))
    dominant_freq = float(freqs[max_idx])
    max_magnitude = float(magnitudes[max_idx])

    if max_magnitude < threshold:
        return EVENT_TYPE_NONE, dominant_freq, max_magnitude, 0.0

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
    def __init__(
        self,
        window_size: int = 100,
        fs: int = 20,
        threshold: float = 40.0,
        step: int = 10,
        cooldown_seconds: float = 5.0   # ← NUOVO
    ):
        self.window_size      = window_size
        self.fs               = fs
        self.threshold        = threshold
        self.step             = step
        self.cooldown_seconds = cooldown_seconds  # ← NUOVO

        self.sensor_buffers:   dict[str, deque] = {}
        self._sample_counters: dict[str, int]   = {}
        self._last_emitted:    dict[str, float] = {}  # ← NUOVO: sensor_id → wall-clock time

    def process_incoming_sample(
        self,
        sensor_id: str,
        value: float,
        fs: float,
        timestamp: Optional[str] = None,
        location: Optional[dict] = None,
    ) -> Optional[dict]:

        # Lazy-init buffer e contatore
        if sensor_id not in self.sensor_buffers:
            self.sensor_buffers[sensor_id]   = deque(maxlen=self.window_size)
            self._sample_counters[sensor_id] = 0

        self.sensor_buffers[sensor_id].append(value)
        self._sample_counters[sensor_id] += 1

        # Aspetta finestra piena
        if len(self.sensor_buffers[sensor_id]) < self.window_size:
            return None

        # Analisi FFT solo ogni `step` sample
        if self._sample_counters[sensor_id] % self.step != 0:
            return None

        window_data = np.array(self.sensor_buffers[sensor_id])
        event_type, dominant_freq, magnitude, severity = analyze_seismic_window(
            window_data, fs=fs, threshold=self.threshold
        )

        if event_type == EVENT_TYPE_NONE:
            return None

        # ← NUOVO: cooldown per-sensore — evita burst di detections sullo stesso evento fisico
        now  = time.time()
        last = self._last_emitted.get(sensor_id, 0.0)
        if now - last < self.cooldown_seconds:
            return None
        self._last_emitted[sensor_id] = now

        sample_ts = timestamp or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        # Round to 10-second bucket so all replicas produce the same eventId
        # for the same physical event, regardless of FFT trigger offset
        from datetime import datetime as _dt
        try:
            dt = _dt.fromisoformat(sample_ts.replace("Z", "+00:00"))
            bucketed = dt.replace(second=(dt.second // 10) * 10, microsecond=0)
            ts_seed = bucketed.strftime("%Y-%m-%dT%H:%M:%S")
        except Exception:
            ts_seed = sample_ts[:19]
        return {
            "eventId": str(uuid.uuid5(uuid.NAMESPACE_URL, f"{sensor_id}{ts_seed}{event_type}")),
            "sensorId":           sensor_id,
            "timestamp":          sample_ts,
            "location":           location or {"latitude": 0.0, "longitude": 0.0},
            "dominantFrequencyHz": round(dominant_freq, 2),
            "eventType":          event_type,
            "severityScore":      severity,
            "replicaId":         REPLICA_ID,
            "rawMagnitude":      round(magnitude, 2),
        }


# ==========================================
# 5. STATE INSTANCE
# ==========================================

state = SeismicReplicaState(
    window_size=WINDOW_SIZE,
    fs=FS,
    threshold=THRESHOLD,
    step=FFT_STEP,
    cooldown_seconds=10.0  # match 10s event-ID bucket to prevent duplicate detections
)


# ==========================================
# CIRCUIT BREAKER (US-18)
# ==========================================

class CircuitBreaker:
    def __init__(self, failure_threshold: int = 5, recovery_timeout: float = 30.0):
        self.failure_threshold = failure_threshold
        self.recovery_timeout  = recovery_timeout
        self._failures         = 0
        self._state            = "CLOSED"
        self._opened_at: float = 0.0

    @property
    def state(self) -> str:
        if self._state == "OPEN":
            if time.monotonic() - self._opened_at >= self.recovery_timeout:
                self._state = "HALF-OPEN"
                print("[circuit-breaker] → HALF-OPEN: testing recovery...")
        return self._state

    def record_success(self):
        self._failures = 0
        if self._state != "CLOSED":
            print("[circuit-breaker] → CLOSED: persistence layer recovered ✓")
        self._state = "CLOSED"

    def record_failure(self):
        self._failures += 1
        if self._failures >= self.failure_threshold:
            self._state     = "OPEN"
            self._opened_at = time.monotonic()
            print(
                f"[circuit-breaker] → OPEN after {self._failures} failures. "
                f"Retrying in {self.recovery_timeout}s."
            )

    def is_open(self) -> bool:
        return self.state == "OPEN"


_circuit_breaker = CircuitBreaker(failure_threshold=5, recovery_timeout=30.0)


# ==========================================
# 6. ASYNC TASKS
# ==========================================

async def persist_event(event: dict) -> None:
    if _circuit_breaker.is_open():
        print(f"[circuit-breaker] OPEN — dropping event {event.get('eventId')}")
        return

    async with httpx.AsyncClient() as client:
        try:
            response = await client.post(
                f"{DB_SERVICE_URL}/events",
                json=event,
                timeout=5.0
            )
            response.raise_for_status()
            _circuit_breaker.record_success()
        except (httpx.HTTPStatusError, httpx.RequestError, Exception) as e:
            _circuit_breaker.record_failure()
            print(f"[persist] Failed to store event {event.get('eventId')}: {e}")


async def listen_to_broker() -> None:
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
                    location  = msg.get("location")
                    fs_rate   = float(msg.get("sampling_rate_hz", FS))

                    if sensor_id is None or value is None:
                        continue

                    detection = state.process_incoming_sample(
                        sensor_id=sensor_id,
                        value=float(value),
                        fs=fs_rate,
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
    url = f"{SIMULATOR_URL}/api/control"

    while True:
        try:
            timeout = httpx.Timeout(None)
            async with httpx.AsyncClient(timeout=timeout) as client:
                async with client.stream("GET", url) as response:
                    current_event_type = None

                    async for line in response.aiter_lines():
                        line = line.strip()

                        if line.startswith("event:"):
                            current_event_type = line[6:].strip()

                        elif line.startswith(""):
                            if current_event_type != "command":
                                continue
                            payload_str = line[5:].strip()
                            if not payload_str:
                                continue
                            try:
                                payload = json.loads(payload_str)
                                if payload.get("command") == "SHUTDOWN":
                                    print("[control] SHUTDOWN received. Terminating replica.")
                                    os._exit(1)
                            except json.JSONDecodeError:
                                continue

                        elif line == "":
                            current_event_type = None

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
    return {
        "status":          "ok",
        "replica_id":      REPLICA_ID,
        "sensors_tracked": list(state.sensor_buffers.keys()),
        "buffer_sizes":    {k: len(v) for k, v in state.sensor_buffers.items()},
    }

if __name__ == "__main__":
    uvicorn.run("processing_replicas:app", host="0.0.0.0", port=8080, reload=False)

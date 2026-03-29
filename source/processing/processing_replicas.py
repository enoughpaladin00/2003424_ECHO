# ==========================================
# 1. IMPORTS
# ==========================================
import asyncio
import os
import sys
import time
import json
import httpx
import websockets
import numpy as np
from collections import deque
from contextlib import asynccontextmanager
from fastapi import FastAPI
import uvicorn
from typing import Optional

# ==========================================
# 2. CORE MATH LOGIC (FFT & Classification)
# ==========================================
def analyze_seismic_window(window, fs=20, threshold=50.0):
    """
    Analyze a seismic time-domain window using FFT.
    
    Args:
        window: array of seismic samples (mm/s)
        fs: sampling frequency in Hz (default: 20 Hz as per simulator contract)
        threshold: minimum magnitude to consider a detection valid (noise floor)
    
    Returns:
        (event_type, dominant_freq, max_magnitude)
    """
    # Subtract mean to remove DC offset (sensor bias) before FFT
    # This is cleaner than zeroing out bin[0] post-FFT
    window = np.asarray(window, dtype=float)
    window = window - np.mean(window)

    # Compute real FFT and corresponding frequency bins
    freqs = np.fft.rfftfreq(len(window), d=1/fs)
    magnitudes = np.abs(np.fft.rfft(window))

    # Exclude DC component (index 0) — should already be ~0 after mean removal,
    # but we zero it explicitly as a safeguard
    magnitudes[0] = 0.0

    # Find dominant frequency
    max_idx = np.argmax(magnitudes)
    dominant_freq = freqs[max_idx]
    max_magnitude = magnitudes[max_idx]

    # Noise gate: skip classification if signal is too weak
    if max_magnitude < threshold:
        return "No significant event", dominant_freq, max_magnitude

    # Classify based on dominant frequency band
    if 0.5 <= dominant_freq < 3.0:
        event = "Earthquake"
    elif 3.0 <= dominant_freq < 8.0:
        event = "Conventional Explosion"
    elif dominant_freq >= 8.0:
        event = "Nuclear-like Event"
    else:
        event = "Unclassified Anomaly"

    return event, dominant_freq, max_magnitude




# ==========================================
# 3. SLIDING WINDOW STATE
# ==========================================
class SeismicReplicaState:
    """
    Manages per-sensor sliding windows and runs FFT-based event detection
    on each incoming sample. Designed to be instantiated once per processing replica.
    """

    def __init__(self, window_size: int = 100, fs: int = 20, threshold: float = 80.0,
                 step: int = 10):
        """
        Args:
            window_size: number of samples per analysis window (100 @ 20 Hz = 5 s)
            fs: sampling frequency in Hz — must match simulator SAMPLING_RATE_HZ
            threshold: minimum FFT magnitude to trigger an event (noise gate)
            step: run FFT every `step` new samples instead of every single sample.
                  Avoids redundant computation on overlapping windows (hop size).
        """
        self.window_size = window_size
        self.fs = fs
        self.threshold = threshold
        self.step = step

        # { sensor_id: deque(maxlen=window_size) }
        self.sensor_buffers: dict[str, deque] = {}

        # Tracks how many samples have arrived since last FFT run per sensor
        self._sample_counters: dict[str, int] = {}

    def process_incoming_sample(self, sensor_id: str, value: float,
                                timestamp: Optional[str] = None) -> Optional[dict]:
        """
        Call this for every measurement received from the broker.
        Returns a detection dict if an event is found, otherwise None.
        The returned dict is ready to be forwarded to the persistence layer.
        """
        # Lazy-initialise buffer and counter for new sensors
        if sensor_id not in self.sensor_buffers:
            self.sensor_buffers[sensor_id] = deque(maxlen=self.window_size)
            self._sample_counters[sensor_id] = 0

        self.sensor_buffers[sensor_id].append(value)
        self._sample_counters[sensor_id] += 1

        # Wait until we have a full window before running any analysis
        if len(self.sensor_buffers[sensor_id]) < self.window_size:
            return None

        # Only run FFT every `step` samples (sliding window with hop)
        if self._sample_counters[sensor_id] % self.step != 0:
            return None

        window_data = np.array(self.sensor_buffers[sensor_id])
        event, freq, mag = analyze_seismic_window(window_data, fs=self.fs,
                                                   threshold=self.threshold)
        if event == "No significant event":
            return None

        return {
            "sensor_id":   sensor_id,
            "event_type":  event,
            "dominant_hz": round(freq, 2),
            "magnitude":   round(mag, 2),
            "timestamp":   timestamp or time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "replica_id":  REPLICA_ID,
        
        }
    
# ==========================================
# 4. CONFIGURATION
# ==========================================

# Configuration from environment variables (set in docker-compose)
BROKER_WS_URL    = os.getenv("BROKER_WS_URL",    "ws://localhost:8000")
SIMULATOR_URL    = os.getenv("SIMULATOR_URL",    "http://localhost:8080")  #BROKER_WS_URL    = os.getenv("BROKER_WS_URL",    "ws://broker:8000/ws")
                                                                            #SIMULATOR_URL    = os.getenv("SIMULATOR_URL",    "http://simulator:8080")
DB_SERVICE_URL   = os.getenv("DB_SERVICE_URL",   "http://persistence:8001")
WINDOW_SIZE      = int(os.getenv("WINDOW_SIZE",  "100"))
FFT_STEP         = int(os.getenv("FFT_STEP",     "10"))
THRESHOLD        = float(os.getenv("THRESHOLD",  "80.0"))
FS               = int(os.getenv("FS",           "20"))
REPLICA_ID       = os.getenv("REPLICA_ID",      "replica-?")


# ==========================================
# 5. STATE INSTANCE
# ==========================================

# One shared state object per replica instance
state = SeismicReplicaState(
    window_size=WINDOW_SIZE,
    fs=FS,
    threshold=THRESHOLD,
    step=FFT_STEP
)

# ==========================================
# 6. ASYNC TASKS
# ==========================================
async def persist_event(event: dict):
    """
    Fire-and-forget POST to the persistence service.
    The persistence layer is responsible for duplicate-safe insertion.
    """
    async with httpx.AsyncClient() as client:
        try:
            await client.post(f"{DB_SERVICE_URL}/events", json=event, timeout=5.0)
        except Exception as e:
            print(f"[persist] Failed to store event: {e}")


async def listen_to_broker():
    """
    Maintains a persistent WebSocket connection to the broker.
    Reconnects automatically on transient failures.
    Each message is a JSON object: { sensor_id, timestamp, value }
    """
    print(f"[broker] Connecting to {BROKER_WS_URL}")
    while True:
        try:
            async with websockets.connect(BROKER_WS_URL) as ws:
                print("[broker] Connected.")
                async for raw_msg in ws:
                    msg = json.loads(raw_msg)
                    sensor_id = msg["sensor_id"]
                    value     = float(msg["value"])
                    timestamp = msg.get("timestamp")

                    detection = state.process_incoming_sample(sensor_id, value, timestamp)
                    if detection:
                        print(f"[detection] {detection}")
                        asyncio.create_task(persist_event(detection))

        except Exception as e:
            print(f"[broker] Connection lost: {e}. Retrying in 3s...")
            await asyncio.sleep(3)


async def listen_for_shutdown():
    url = f"{SIMULATOR_URL}/api/control"
    while True:
        try:
            async with httpx.AsyncClient(timeout=None) as client:
                async with client.stream("GET", url) as response:
                    async for line in response.aiter_lines():
                        # SSE lines carrying data always start with ""
                        # Using startswith("") covers both " {}"
                        # and "{}" — more robust than requiring the space
                        if not line.startswith(""):
                            continue

                        # Strip the prefix and any surrounding whitespace
                        # to get a clean JSON string regardless of spacing
                        payload_str = line.removeprefix("").strip()

                        try:
                            payload = json.loads(payload_str)
                        except json.JSONDecodeError:
                            continue  # skip malformed or empty data lines silently

                        if payload.get("command") == "SHUTDOWN":
                            print("[control] SHUTDOWN received. Terminating replica.")
                            sys.exit(0)  # forced shutdown as per spec

        except Exception as e:
            print(f"[control] SSE stream lost: {e}. Retrying in 3s...")
            await asyncio.sleep(3)



# ==========================================
# 7. FASTAPI APP
# ==========================================

@asynccontextmanager
async def lifespan(app: FastAPI):
    # Launch both background tasks when the server starts
    asyncio.create_task(listen_to_broker())
    asyncio.create_task(listen_for_shutdown())
    yield

app = FastAPI(lifespan=lifespan)

@app.get("/health")
def health():
    return {
        "status": "ok",
        "sensors_tracked": list(state.sensor_buffers.keys()),
        "buffer_sizes": {k: len(v) for k, v in state.sensor_buffers.items()}
    }


if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8002, reload=False)
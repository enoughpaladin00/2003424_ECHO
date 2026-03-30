# main.py
# E.C.H.O. Seismic Broker — Ingestion + Fan-out layer
# Covers: sensor discovery, WebSocket ingestion, exponential backoff, DLQ, fan-out server

# ==========================================
# 1. IMPORTS
# ==========================================

import asyncio
import json
import logging
import os
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiohttp
import websockets

# ==========================================
# 2. LOGGING
# ==========================================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ==========================================
# 3. CONFIGURATION
# ==========================================

SIMULATOR_BASE_URL  = os.getenv("SIMULATOR_URL", "http://seismic-simulator:8080")
DEVICES_ENDPOINT    = f"{SIMULATOR_BASE_URL}/api/devices/"

BACKOFF_BASE        = 2     # exponential base in seconds: 2^n
BACKOFF_MAX         = 60    # maximum wait between reconnection attempts (seconds)
STABLE_THRESHOLD    = 30    # seconds of uptime before backoff resets on drop

DLQ_FILE            = "dlq.log"
_dlq_lock           = asyncio.Lock()   # prevents concurrent writes corrupting the log

REPLICA_SERVER_HOST = "0.0.0.0"
REPLICA_SERVER_PORT = int(os.getenv("BROKER_PORT", "8000"))

# Global set of active replica WebSocket connections (processing layer)
_active_replicas: set[websockets.WebSocketServerProtocol] = set()

# ==========================================
# 4. DATA MODEL
# ==========================================

@dataclass
class SensorInfo:
    """Holds all information needed to connect to a single sensor."""
    sensor_id:     str
    websocket_url: str    # relative path,  e.g. /api/device/sensor-08/ws
    full_ws_url:   str    # full WS address, e.g. ws://localhost:8080/api/device/sensor-08/ws
    location:      dict   # {"latitude": float, "longitude": float} — forwarded to replicas
    raw:           dict   # original JSON payload from /api/devices/, kept for debugging

# ==========================================
# 5. DISCOVERY
# ==========================================

async def discover_sensors(session: aiohttp.ClientSession) -> list[SensorInfo]:
    """
    GETs /api/devices/ and returns the list of active sensors.
    Extracts location from the device payload so the broker can enrich
    every forwarded message with geographical coordinates.

    Returns:
        List of SensorInfo, one per discovered device.
    Raises:
        aiohttp.ClientError: if the simulator is unreachable.
    """
    logger.info(f"Starting sensor discovery → {DEVICES_ENDPOINT}")

    async with session.get(DEVICES_ENDPOINT) as response:
        response.raise_for_status()
        devices_raw: list[dict] = await response.json()

    sensors = []
    for device in devices_raw:
        sensor_id   = device["id"]
        ws_path     = device["websocket_url"]
        full_ws_url = f"ws://{SIMULATOR_BASE_URL.split('://')[-1]}{ws_path}"

        # Extract location if the simulator provides it; default to 0,0 otherwise.
        # Replicas need this to build the schema-compliant event payload.
        location = device.get("location", {"latitude": 0.0, "longitude": 0.0})

        sensors.append(SensorInfo(
            sensor_id     = sensor_id,
            websocket_url = ws_path,
            full_ws_url   = full_ws_url,
            location      = location,
            raw           = device
        ))
        logger.info(
            f"  ✓ Discovered: {sensor_id} → {full_ws_url} "
            f"| lat={location.get('latitude')} lon={location.get('longitude')}"
        )

    logger.info(f"Discovery complete: {len(sensors)} sensor(s) found.")
    return sensors

# ==========================================
# 6. DEAD-LETTER QUEUE
# ==========================================

async def write_to_dlq(sensor_id: str, raw_message: str, reason: str) -> None:
    """
    Appends a malformed or schema-invalid message to the Dead-Letter Queue file.
    Uses a lock to prevent interleaved writes from concurrent sensor tasks.
    """
    entry = {
        "timestamp_dlq": datetime.now(timezone.utc).isoformat(),
        "sensor_id":     sensor_id,
        "reason":        reason,
        "raw_payload":   raw_message,
    }
    async with _dlq_lock:
        with open(DLQ_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

    logger.warning(
        f"[{sensor_id}] → DLQ | reason: {reason} | payload: {raw_message[:80]}"
    )


def validate_message(data: dict) -> tuple[bool, str]:
    """
    Checks that a parsed message contains the required fields with correct types.

    Returns:
        (True, "")           if the message is valid.
        (False, reason_str)  if a field is missing or has the wrong type.
    """
    if "timestamp" not in data:
        return False, "missing field 'timestamp'"
    if "value" not in data:
        return False, "missing field 'value'"
    if not isinstance(data["value"], (int, float)):
        return False, f"'value' is not numeric: got {type(data['value']).__name__}"
    return True, ""
# ==========================================
# 7. MESSAGE READER (inner WS loop)
# ==========================================

async def read_messages(
    ws,
    sensor: SensorInfo,
    output_queue: asyncio.Queue
) -> None:
    """
    Consumes messages from an open WebSocket connection (one sensor).

    For each message:
    - Attempts JSON parse → DLQ on failure
    - Validates required fields → DLQ on failure
    - Enriches the payload with sensor_id, ingested_at, and location
    - Puts the enriched message on the fan-out queue

    The location field ensures downstream replicas can build a fully
    schema-compliant E.C.H.O. event without querying the simulator again.
    """
    async for raw_message in ws:

        # Parse JSON
        try:
            data = json.loads(raw_message)
        except json.JSONDecodeError as e:
            await write_to_dlq(sensor.sensor_id, raw_message, f"malformed JSON: {e}")
            continue

        # Validate required fields
        is_valid, reason = validate_message(data)
        if not is_valid:
            await write_to_dlq(sensor.sensor_id, raw_message, reason)
            continue

        # Enrich — add broker-side metadata and geographical coordinates
        data["sensor_id"]   = sensor.sensor_id
        data["value"]       = float(data["value"])   # normalise to native float
        data["ingested_at"] = datetime.now(timezone.utc).isoformat()
        data["location"]    = sensor.location        # lat/lon from discovery payload

        await output_queue.put(data)

# ==========================================
# 8. INGESTION LOOP (outer reconnect loop)
# ==========================================

async def sensor_ingestion_loop(
    sensor: SensorInfo,
    output_queue: asyncio.Queue
) -> None:
    """
    Persistent connection loop for a single sensor (US-5, US-7).
    Reconnects automatically using exponential backoff: wait = min(2^n, BACKOFF_MAX).
    Backoff counter resets to 0 if the connection was stable for ≥ STABLE_THRESHOLD s,
    so a briefly-flapping sensor is not penalised with a 60-second wait.
    """
    attempt = 0

    while True:
        if attempt > 0:
            wait_time = min(BACKOFF_BASE ** attempt, BACKOFF_MAX)
            logger.warning(
                f"[{sensor.sensor_id}] Attempt #{attempt} — "
                f"waiting {wait_time}s before reconnecting..."
            )
            await asyncio.sleep(wait_time)

        connect_time = None

        try:
            logger.info(f"[{sensor.sensor_id}] Connecting → {sensor.full_ws_url}")

            async with websockets.connect(sensor.full_ws_url) as ws:
                connect_time = asyncio.get_event_loop().time()
                logger.info(f"[{sensor.sensor_id}] ✓ Connected.")
                attempt = 0   # reset immediately on successful handshake

                await read_messages(ws, sensor, output_queue)

        except (websockets.ConnectionClosedError, websockets.ConnectionClosedOK) as e:
            elapsed = asyncio.get_event_loop().time() - (connect_time or 0)
            if elapsed >= STABLE_THRESHOLD:
                attempt = 0
                logger.info(
                    f"[{sensor.sensor_id}] Dropped after {elapsed:.0f}s stable "
                    f"— resetting backoff."
                )
            else:
                attempt += 1
            logger.warning(f"[{sensor.sensor_id}] WebSocket closed: {e}")

        except (OSError, websockets.InvalidURI, websockets.WebSocketException) as e:
            attempt += 1
            logger.error(f"[{sensor.sensor_id}] Connection error: {e}")

# ==========================================
# 9. FAN-OUT: REPLICA SERVER + DISPATCHER
# ==========================================

async def replica_handler(websocket) -> None:
    """
    Accepts an incoming WebSocket connection from a processing replica (US-6, US-17).
    Registers it in _active_replicas and keeps the connection alive.
    Replicas are receive-only consumers; any messages they send are silently ignored.
    """
    replica_addr = websocket.remote_address
    logger.info(f"[Server] New replica connected: {replica_addr}")
    _active_replicas.add(websocket)

    try:
        async for _ in websocket:
            pass
    except websockets.ConnectionClosed:
        pass
    finally:
        _active_replicas.discard(websocket)   # discard: safe even if already removed
        logger.warning(f"[Server] Replica disconnected: {replica_addr}")


async def fanout_dispatcher(output_queue: asyncio.Queue) -> None:
    """
    Broadcasts validated, enriched seismic measurements to all connected replicas (US-6).

    Design notes:
    - Snapshots _active_replicas before iterating to avoid RuntimeError
      if a replica disconnects mid-broadcast.
    - return_exceptions=True in gather() prevents one dead replica from
      aborting the entire broadcast round.
    - Messages are dropped (not buffered) if no replica is currently connected;
      the broker is a real-time relay, not a message store.
    """
    while True:
        data = await output_queue.get()
        message_str = json.dumps(data)

        if _active_replicas:
            snapshot   = list(_active_replicas)   # atomic snapshot
            send_tasks = [
                asyncio.create_task(ws.send(message_str)) for ws in snapshot
            ]
            await asyncio.gather(*send_tasks, return_exceptions=True)
        else:
            logger.debug(
                f"[fanout] No replicas connected — message from "
                f"{data.get('sensor_id')} dropped."
            )

        output_queue.task_done()

# ==========================================
# 10. ENTRY POINT
# ==========================================

async def main() -> None:
    output_queue = asyncio.Queue(maxsize=1000)   # back-pressure buffer toward fan-out

    # --- Discovery: fetch sensor list from simulator ---
    async with aiohttp.ClientSession() as session:
        sensors = await discover_sensors(session)

    if not sensors:
        logger.error("No sensors found. Is the simulator running?")
        return

    # --- Ingestion: one independent async task per sensor (US-5) ---
    ingestion_tasks = [
        asyncio.create_task(
            sensor_ingestion_loop(s, output_queue),
            name=f"ingestion-{s.sensor_id}"
        )
        for s in sensors
    ]

    # --- Fan-out server: listen for replica registrations (US-6, US-17) ---
    logger.info(
        f"Starting replica WebSocket server on "
        f"ws://{REPLICA_SERVER_HOST}:{REPLICA_SERVER_PORT}"
    )
    server = await websockets.serve(
        replica_handler,
        REPLICA_SERVER_HOST,
        REPLICA_SERVER_PORT
    )

    # --- Dispatcher: broadcast queue → all active replicas ---
    dispatcher_task = asyncio.create_task(
        fanout_dispatcher(output_queue),
        name="fanout-dispatcher"
    )

    logger.info("Broker fully started. Waiting for replicas and sensor data...")

    await asyncio.gather(*ingestion_tasks, dispatcher_task)
    server.close()


if __name__ == "__main__":
    asyncio.run(main())

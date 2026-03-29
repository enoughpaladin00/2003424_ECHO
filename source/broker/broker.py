# main.py
# Seismic Broker — Ingestion + Fan-out layer
# Covers: sensor discovery, WebSocket ingestion, exponential backoff, DLQ, fan-out server

import asyncio
import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

import aiohttp
import websockets

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)
logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Configuration
# ---------------------------------------------------------------------------

SIMULATOR_BASE_URL = "http://localhost:8080"
DEVICES_ENDPOINT   = f"{SIMULATOR_BASE_URL}/api/devices/"

BACKOFF_BASE     = 2    # exponential base in seconds
BACKOFF_MAX      = 60   # maximum wait between reconnection attempts (seconds)
STABLE_THRESHOLD = 30   # if connected longer than this, reset backoff on drop

DLQ_FILE  = "dlq.log"
_dlq_lock = asyncio.Lock()

REPLICA_SERVER_HOST = "0.0.0.0"
REPLICA_SERVER_PORT = 8000

# Global set of active replica connections (processing layer — Emanuele's replicas)
_active_replicas: set[websockets.WebSocketServerProtocol] = set()

# ---------------------------------------------------------------------------
# Data model
# ---------------------------------------------------------------------------

@dataclass
class SensorInfo:
    """Holds all information needed to connect to a single sensor."""
    sensor_id:     str
    websocket_url: str   # relative path, e.g. /api/device/sensor-08/ws
    full_ws_url:   str   # full WS address, e.g. ws://localhost:8080/api/device/sensor-08/ws
    raw:           dict  # original JSON payload, kept for debugging

# ---------------------------------------------------------------------------
# Discovery
# ---------------------------------------------------------------------------

async def discover_sensors(session: aiohttp.ClientSession) -> list[SensorInfo]:
    """
    GETs /api/devices/ and returns the list of active sensors.

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
        full_ws_url = f"ws://localhost:8080{ws_path}"

        sensors.append(SensorInfo(
            sensor_id     = sensor_id,
            websocket_url = ws_path,
            full_ws_url   = full_ws_url,
            raw           = device
        ))
        logger.info(f"  ✓ Discovered sensor: {sensor_id} → {full_ws_url}")

    logger.info(f"Discovery complete: {len(sensors)} sensor(s) found.")
    return sensors

# ---------------------------------------------------------------------------
# Dead-Letter Queue
# ---------------------------------------------------------------------------

async def write_to_dlq(sensor_id: str, raw_message: str, reason: str) -> None:
    """Appends a malformed or invalid message to the Dead-Letter Queue file."""
    entry = {
        "timestamp_dlq": datetime.now(timezone.utc).isoformat(),
        "sensor_id":     sensor_id,
        "reason":        reason,
        "raw_payload":   raw_message,
    }
    async with _dlq_lock:
        with open(DLQ_FILE, "a", encoding="utf-8") as f:
            f.write(json.dumps(entry) + "\n")

    logger.warning(f"[{sensor_id}] → DLQ | reason: {reason} | payload: {raw_message[:80]}")


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

# ---------------------------------------------------------------------------
# Message reader (inner loop — runs while the WS connection is alive)
# ---------------------------------------------------------------------------

async def read_messages(
    ws,
    sensor: SensorInfo,
    output_queue: asyncio.Queue
) -> None:
    """
    Consumes messages from an open WebSocket connection.
    - Valid messages are enriched and pushed to the fan-out queue.
    - Malformed or invalid messages are routed to the DLQ.
    """
    async for raw_message in ws:

        # Attempt JSON parse
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

        # Enrich and forward to fan-out queue
        data["sensor_id"]   = sensor.sensor_id
        data["ingested_at"] = datetime.now(timezone.utc).isoformat()
        await output_queue.put(data)

# ---------------------------------------------------------------------------
# Ingestion loop (outer loop — handles reconnections with exponential backoff)
# ---------------------------------------------------------------------------

async def sensor_ingestion_loop(
    sensor: SensorInfo,
    output_queue: asyncio.Queue
) -> None:
    """
    Persistent connection loop for a single sensor.
    Reconnects automatically using exponential backoff (2^n seconds, capped at BACKOFF_MAX).
    Backoff resets to 0 if the connection was stable for at least STABLE_THRESHOLD seconds.
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
            logger.info(f"[{sensor.sensor_id}] Connecting to {sensor.full_ws_url} ...")

            async with websockets.connect(sensor.full_ws_url) as ws:
                connect_time = asyncio.get_event_loop().time()
                logger.info(f"[{sensor.sensor_id}] ✓ Connected.")
                attempt = 0

                await read_messages(ws, sensor, output_queue)

        except (websockets.ConnectionClosedError, websockets.ConnectionClosedOK) as e:
            elapsed = asyncio.get_event_loop().time() - (connect_time or 0)
            if elapsed >= STABLE_THRESHOLD:
                attempt = 0
                logger.info(
                    f"[{sensor.sensor_id}] Connection dropped after "
                    f"{elapsed:.0f}s stable — resetting backoff."
                )
            else:
                attempt += 1
            logger.warning(f"[{sensor.sensor_id}] WebSocket closed: {e}")

        except (OSError, websockets.InvalidURI, websockets.WebSocketException) as e:
            attempt += 1
            logger.error(f"[{sensor.sensor_id}] Connection error: {e}")

# ---------------------------------------------------------------------------
# Fan-out: replica server + dispatcher
# ---------------------------------------------------------------------------

async def replica_handler(websocket) -> None:
    """
    Accepts an incoming WebSocket connection from a processing replica (Emanuele).
    Keeps the connection alive until the replica disconnects.
    """
    replica_addr = websocket.remote_address
    logger.info(f"[Server] New replica connected from {replica_addr}")
    _active_replicas.add(websocket)

    try:
        async for _ in websocket:
            pass  # replicas are receive-only; incoming messages are ignored
    except websockets.ConnectionClosed:
        pass
    finally:
        _active_replicas.discard(websocket)  # discard: safe even if already removed
        logger.warning(f"[Server] Replica disconnected: {replica_addr}")


async def fanout_dispatcher(output_queue: asyncio.Queue) -> None:
    """
    Reads validated seismic events from the queue and broadcasts them
    to all currently connected processing replicas.
    """
    while True:
        data = await output_queue.get()
        message_str = json.dumps(data)

        if _active_replicas:
            # Snapshot the set before iterating to avoid RuntimeError
            # if a replica disconnects mid-iteration
            snapshot = list(_active_replicas)
            send_tasks = [
                asyncio.create_task(ws.send(message_str)) for ws in snapshot
            ]
            # return_exceptions=True prevents one dead replica from
            # cancelling the entire broadcast
            await asyncio.gather(*send_tasks, return_exceptions=True)

        output_queue.task_done()

# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

async def main() -> None:
    output_queue = asyncio.Queue(maxsize=1000)

    # --- Step 2: discover all active sensors ---
    async with aiohttp.ClientSession() as session:
        sensors = await discover_sensors(session)

    if not sensors:
        logger.error("No sensors found. Is the simulator running?")
        return

    # --- Step 3 & 4: one ingestion task per sensor ---
    ingestion_tasks = [
        asyncio.create_task(
            sensor_ingestion_loop(s, output_queue),
            name=f"ingestion-{s.sensor_id}"
        )
        for s in sensors
    ]

    # --- Step 5: start the WebSocket server for Emanuele's replicas ---
    logger.info(
        f"Starting replica WebSocket server on "
        f"ws://{REPLICA_SERVER_HOST}:{REPLICA_SERVER_PORT}"
    )
    server = await websockets.serve(
        replica_handler,
        REPLICA_SERVER_HOST,
        REPLICA_SERVER_PORT
    )

    # --- Step 6: start the fan-out dispatcher ---
    dispatcher_task = asyncio.create_task(
        fanout_dispatcher(output_queue),
        name="fanout-dispatcher"
    )

    logger.info("Broker fully started. Waiting for replicas and sensor data...")

    await asyncio.gather(*ingestion_tasks, dispatcher_task)
    server.close()


if __name__ == "__main__":
    asyncio.run(main())

# ==========================================
# E.C.H.O. Processing Replica — Dockerfile
# Emanuele Smisi — Signal Processing
# ==========================================

# --- Stage 1: Builder ---
FROM python:3.12-slim AS builder

WORKDIR /app

# Install build deps (numpy needs gcc on slim)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc curl\
    && rm -rf /var/lib/apt/lists/*

# Copy and install Python dependencies first (layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# --- Stage 2: Runtime ---
FROM python:3.12-slim AS runtime

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY processing_replicas.py .

# ==========================================
# Environment variables (overridable via docker-compose)
# ==========================================
ENV BROKER_WS_URL="ws://ingestion-broker:8000" \
    SIMULATOR_URL="http://seismic-simulator:8080" \
    DB_SERVICE_URL="http://backend-api:3000" \
    WINDOW_SIZE="100" \
    FFT_STEP="10" \
    THRESHOLD="80.0" \
    FS="20" \
    REPLICA_ID="replica-?"

EXPOSE 8080

# Healthcheck for docker-compose and gateway
HEALTHCHECK --interval=15s --timeout=5s --retries=3 \
    CMD python -c "import urllib.request; urllib.request.urlopen('http://localhost:8002/health')"

ENTRYPOINT ["python", "-m", "uvicorn", "processing_replicas:app", \
            "--host", "0.0.0.0", "--port", "8080"]

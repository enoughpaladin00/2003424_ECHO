FROM python:3.11-slim

WORKDIR /app

# Install curl so the docker-compose healthcheck can run
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

# Copy the requirements and install them
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy the actual Python application
COPY percistence_api.py .

# Run the FastAPI server via Uvicorn
CMD ["uvicorn", "percistence_api:app", "--host", "0.0.0.0", "--port", "3000"]
FROM python:3.12-slim

WORKDIR /app

# curl needed for docker-compose healthcheck
RUN apt-get update && apt-get install -y --no-install-recommends curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY persistence_api.py .

EXPOSE 3000

CMD ["uvicorn", "persistence_api:app", "--host", "0.0.0.0", "--port", "3000"]
    
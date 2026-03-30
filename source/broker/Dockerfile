# Usa un'immagine Python ufficiale leggera
FROM python:3.11-slim

# Imposta la directory di lavoro nel container
WORKDIR /app

# Installa le dipendenze necessarie per il broker
RUN pip install --no-cache-dir aiohttp websockets

# Copia il codice sorgente nel container 
# (Assicurati che il file locale si chiami broker.py e non broker(1).py)
COPY broker.py .

# Esponi la porta 8000 (come definito nel docker-compose e in broker.py)
EXPOSE 8000

# Avvia il broker
CMD ["python", "broker.py"]
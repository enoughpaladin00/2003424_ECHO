FROM python:3.9-slim

# Imposta la cartella di lavoro
WORKDIR /app

# Installa Flask dentro il container
RUN pip install --no-cache-dir flask flask-cors

# Copia il file gateway.py nella cartella /app del container
COPY gateway.py .

# Espone la porta
EXPOSE 5000

# Avvia il gateway
CMD ["python", "gateway.py"]
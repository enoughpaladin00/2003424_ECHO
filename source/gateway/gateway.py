from flask import Flask
from flask_cors import CORS

app = Flask(__name__)
CORS(app) # Questo serve per permettere alla Dashboard di parlare col Gateway

@app.route('/')
def home():
    return {"status": "ok", "message": "Gateway Sismico Operativo"}

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=5000)
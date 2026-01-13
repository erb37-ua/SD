# Database/EV_Database.py
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import json
import os
import threading
from typing import List, Dict, Optional

app = FastAPI()

# Archivos físicos
DB_FILE = "cp_database.json"
KEY_FILE = "weather_api_key.json"

# Locks para evitar conflictos de escritura
db_lock = threading.Lock()
key_lock = threading.Lock()

# Modelos de datos
class WeatherKeyPayload(BaseModel):
    api_key: str
    updated_at: str

class CPData(BaseModel):
    id: str
    location: Optional[str] = ""
    city: Optional[str] = "Alicante"
    price: Optional[float] = 0.50

# --- ENDPOINTS API KEY ---

@app.get("/weather-key")
def get_weather_key():
    if not os.path.exists(KEY_FILE):
        raise HTTPException(status_code=404, detail="Key file not found")
    
    with key_lock:
        with open(KEY_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

@app.post("/weather-key")
def update_weather_key(payload: WeatherKeyPayload):
    with key_lock:
        with open(KEY_FILE, "w", encoding="utf-8") as f:
            json.dump(payload.dict(), f)
    return {"status": "OK", "message": "Key updated"}

# --- ENDPOINTS CP DATABASE ---

@app.get("/cps")
def get_all_cps():
    if not os.path.exists(DB_FILE):
        return []
    with db_lock:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)

@app.post("/cps")
def update_cp_database(cps: List[CPData]):
    """Sobrescribe la base de datos completa con la nueva lista"""
    with db_lock:
        data = [cp.dict() for cp in cps]
        with open(DB_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
    return {"status": "OK", "count": len(cps)}

if __name__ == "__main__":
    # Escucha en el puerto 6000
    uvicorn.run(app, host="0.0.0.0", port=6000)
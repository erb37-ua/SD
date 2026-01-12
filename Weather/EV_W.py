import time
import requests
import sys
import os
import json
from urllib.parse import urlparse

# --- CONFIGURACIÓN ---
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

CENTRAL_URL = os.getenv("CENTRAL_URL", "http://localhost:8000")

if os.path.exists("/app/Central/cp_database.json"):
    DB_FILE_PATH = "/app/Central/cp_database.json"
else:
    DB_FILE_PATH = "../Central/cp_database.json"

cp_weather_state = {} 
cp_locations = {}
API_KEY_PATHS = [
    "/app/Central/weather_api_key.json",
    "../Central/weather_api_key.json",
]

def normalize_cp_id(value):
    return (value or "").strip().upper()

def is_valid_cp_id(value):
    cp_id = normalize_cp_id(value)
    return bool(cp_id) and cp_id.isalnum()

def load_weather_api_key():
    env_key = os.getenv("OPENWEATHER_API_KEY")
    if env_key:
        key = env_key.strip()
        return key if len(key) >= 10 else None

    for path in API_KEY_PATHS:
        if not os.path.exists(path):
            continue
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            key = data.get("api_key")
            if isinstance(key, str):
                key = key.strip()
                if len(key) >= 10:
                    return key
        except Exception as exc:
            print(f"[Error] No se pudo leer la API key en {path}: {exc}")
    return None

def load_cp_locations():
    """Lee el JSON compartido para saber qué ciudad corresponde a cada CP."""
    global cp_locations
    if not os.path.exists(DB_FILE_PATH):
        print(f"[Error] No encuentro la BD en {DB_FILE_PATH}. Usando datos dummy.")
        cp_locations = {"CP001": "Alicante", "CP003": "Oslo"}
        return

    try:
        with open(DB_FILE_PATH, 'r') as f:
            data = json.load(f)
            for item in data:
                cp_id = normalize_cp_id(item.get("id"))
                city = (item.get("city") or "Alicante").strip()
                if not is_valid_cp_id(cp_id):
                    print(f"[Warn] CP inválido en BD: {item.get('id')}")
                    continue
                if not city:
                    print(f"[Warn] Ciudad vacía para {cp_id}, usando Alicante.")
                    city = "Alicante"
                cp_locations[cp_id] = city
        print(f"[Info] Ubicaciones cargadas: {len(cp_locations)}")
    except Exception as e:
        print(f"[Error] Leyendo DB: {e}")

def get_temperature(city, api_key):
    """Obtiene temperatura de OpenWeatherMap o simula si no hay API Key."""
    
    if not isinstance(city, str) or not city.strip():
        print("[Warn] Ciudad inválida, se omite.")
        return None

    if not api_key:
        if "Oslo" in city: return -5.0
        return 22.0

    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    try:
        r = requests.get(url, timeout=3)
        if r.status_code == 200:
            return r.json()['main']['temp']
        else:
            print(f"[API Error] {city}: {r.status_code}")
    except Exception as e:
        print(f"[Net Error] {e}")
    
    return -5.0 if "Oslo" in city else 20.0

def notify_central(cp_id, action):
    endpoint = "/api/alert" if action == "STOP" else "/api/resume"
    url = f"{CENTRAL_URL}{endpoint}"
    payload = {
        "cp_id": cp_id, 
        "reason": "Weather Alert" if action == "STOP" else "Weather OK"
    }
    
    print(f"   [Intento] Contactando Central: {url} ...") 
    
    try:
        r = requests.post(url, json=payload, timeout=5)
        if r.status_code == 200:
            print(f"   [-->] Central notificada correctamente: {action}")
            return True
        else:
            print(f"   [Error Central] Código {r.status_code}: {r.text}")
            return False
    except Exception as e:
        print(f"   [Error Conexión] No se pudo conectar a Central: {e}")
        return False


def send_telemetry(cp_id, temp):
    """Envía la temperatura a la Central para que se vea en el Front."""
    url = f"{CENTRAL_URL}/api/weather"
    try:
        requests.post(url, json={"cp_id": cp_id, "temperature": temp}, timeout=2)
    except Exception:
        pass 

def main():
    print("*** EV_W Iniciado ***")
    time.sleep(2) 

    parsed_central = urlparse(CENTRAL_URL)
    if parsed_central.scheme not in ("http", "https") or not parsed_central.netloc:
        print(f"[Error] CENTRAL_URL inválida: {CENTRAL_URL}")
        return
    
    while True:
        load_cp_locations() 
        api_key = load_weather_api_key()

        print("\n--- Analizando Clima ---")
        for cp_id, city in cp_locations.items():
            if not is_valid_cp_id(cp_id):
                print(f"[Warn] CP inválido en memoria: {cp_id}")
                continue
            
            if cp_id not in cp_weather_state:
                cp_weather_state[cp_id] = "OK"

            temp = get_temperature(city, api_key)
            if temp is None:
                continue
            
            send_telemetry(cp_id, temp)

            state = cp_weather_state.get(cp_id, "OK")
            print(f"> {cp_id} ({city}): {temp}ºC")

            if temp < 0 and state == "OK":
                print(f"  [ALERTA] Congelación. Parando {cp_id}...")
                if notify_central(cp_id, "STOP"):
                    cp_weather_state[cp_id] = "BAD"
            
            elif temp >= 0 and state == "BAD":
                print(f"  [MEJORA] Clima OK. Reanudando {cp_id}...")
                if notify_central(cp_id, "RESUME"):
                    cp_weather_state[cp_id] = "OK"
        
        time.sleep(5)

if __name__ == "__main__":
    main()

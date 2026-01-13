import time
import requests
import sys
import os
import json
from urllib.parse import urlparse

# --- CONFIGURACIÓN ---
CENTRAL_URL = os.getenv("CENTRAL_URL", "http://localhost:8000")
DB_HOST = os.getenv("DB_HOST", "http://localhost:6000") # URL del nuevo servidor DB

cp_weather_state = {} 
cp_locations = {}

def normalize_cp_id(value):
    return (value or "").strip().upper()

def is_valid_cp_id(value):
    cp_id = normalize_cp_id(value)
    return bool(cp_id) and cp_id.isalnum()

def load_weather_api_key():
    """Obtiene la API Key directamente del servidor de Base de Datos"""
    # 1. Variable de entorno local (prioridad test)
    env_key = os.getenv("OPENWEATHER_API_KEY")
    if env_key and len(env_key) >= 10:
        return env_key

    # 2. Petición al servidor de BD
    try:
        resp = requests.get(f"{DB_HOST}/weather-key", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            key = data.get("api_key")
            print("Aleatorio: " + key)
            if key and len(key) >= 10:
                return key
    except Exception as e:
        print(f"[Warn] No se pudo obtener Key de DB ({DB_HOST}): {e}")
    
    return None

def load_cp_locations():
    """Obtiene la lista de CPs y ciudades del servidor de BD"""
    global cp_locations
    try:
        resp = requests.get(f"{DB_HOST}/cps", timeout=5)
        if resp.status_code == 200:
            data = resp.json()
            new_locs = {}
            for item in data:
                cp_id = normalize_cp_id(item.get("id"))
                city = item.get("city", "Alicante")
                new_locs[cp_id] = city
            cp_locations = new_locs
            # print(f"[Info] Ubicaciones actualizadas: {len(cp_locations)}")
        else:
            print(f"[Warn] Error obteniendo ubicaciones: {resp.status_code}")
    except Exception as e:
        print(f"[Warn] Error conectando a DB para ubicaciones: {e}")
        # Si falla, mantenemos las antiguas o usamos dummy si está vacío
        if not cp_locations:
            cp_locations = {"CP001": "Alicante"}

def get_temperature(city, api_key):
    if not isinstance(city, str) or not city.strip():
        return None
    if not api_key:
        if "Oslo" in city: return -5.0
        return 22.0
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={api_key}&units=metric"
    try:
        r = requests.get(url, timeout=3)
        if r.status_code == 200:
            return r.json()['main']['temp']
    except Exception:
        pass
    return 20.0

def notify_central(cp_id, action):
    endpoint = "/api/alert" if action == "STOP" else "/api/resume"
    url = f"{CENTRAL_URL}{endpoint}"
    try:
        requests.post(url, json={"cp_id": cp_id, "reason": action}, timeout=5)
        return True
    except:
        return False

def send_telemetry(cp_id, temp):
    try:
        requests.post(f"{CENTRAL_URL}/api/weather", json={"cp_id": cp_id, "temperature": temp}, timeout=2)
    except:
        pass

def main():
    print(f"*** EV_W Iniciado (Conectado a DB: {DB_HOST}) ***")
    time.sleep(5) # Esperar a que DB arranque

    while True:
        load_cp_locations() 
        api_key = load_weather_api_key()

        if api_key:
            print(f"[Info] Usando API Key: {api_key[:4]}***")
        else:
            print("[Info] Sin API Key válida. Simulando.")

        # ... (Resto del bucle for igual que antes) ...
        print("\n--- Analizando Clima ---")
        for cp_id, city in cp_locations.items():
            if not is_valid_cp_id(cp_id): continue
            
            if cp_id not in cp_weather_state: cp_weather_state[cp_id] = "OK"

            temp = get_temperature(city, api_key)
            if temp is None: continue
            
            send_telemetry(cp_id, temp)
            state = cp_weather_state.get(cp_id, "OK")
            print(f"> {cp_id} ({city}): {temp}ºC")

            if temp < 0 and state == "OK":
                if notify_central(cp_id, "STOP"): cp_weather_state[cp_id] = "BAD"
            elif temp >= 0 and state == "BAD":
                if notify_central(cp_id, "RESUME"): cp_weather_state[cp_id] = "OK"
        
        time.sleep(10)

if __name__ == "__main__":
    main()
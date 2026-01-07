import time
import requests
import sys
import os
import json
import argparse

# CONFIGURACIÓN
# Leemos la API KEY del entorno
OPENWEATHER_API_KEY = os.getenv("OPENWEATHER_API_KEY")

# Variable global que se rellenará con el argumento del parser
CENTRAL_URL = None 

# Detectar si estamos en Docker o en local para la BBDD
if os.path.exists("/app/Central/cp_database.json"):
    # Ruta dentro del contenedor Docker
    DB_FILE_PATH = "/app/Central/cp_database.json"
else:
    # Ruta relativa para ejecución local en Windows
    # Asume que ejecutas desde la carpeta Weather/
    DB_FILE_PATH = "../Central/cp_database.json"

# Estado interno
cp_weather_state = {} 
cp_locations = {} 

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
                # Extraemos ID y Ciudad
                cp_id = item.get("id")
                city = item.get("city", "Alicante")
                if cp_id:
                    cp_locations[cp_id] = city
        print(f"[Info] Ubicaciones cargadas: {len(cp_locations)}")
    except Exception as e:
        print(f"[Error] Leyendo DB: {e}")

def get_temperature(city):
    """Obtiene temperatura de OpenWeatherMap o simula si no hay API Key."""
    
    # MODO SIMULACIÓN (si no carga la API Key)
    if not OPENWEATHER_API_KEY:
        if "Oslo" in city: return -5.0
        return 22.0

    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}&units=metric"
    try:
        r = requests.get(url, timeout=3)
        if r.status_code == 200:
            return r.json()['main']['temp']
        else:
            print(f"[API Error] {city}: {r.status_code}")
    except Exception as e:
        print(f"[Net Error] {e}")
    
    # Fallback si falla internet
    return -5.0 if "Oslo" in city else 20.0

def notify_central(cp_id, action):
    endpoint = "/api/alert" if action == "STOP" else "/api/resume"
    url = f"{CENTRAL_URL}{endpoint}"
    payload = {
        "cp_id": cp_id, 
        "reason": "Weather Alert" if action == "STOP" else "Weather OK"
    }
    
    print(f"   [Intento] Contactando Central: {url} ...") # Debug
    
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
        pass # Ignoramos errores de telemetría para no ensuciar el log

def main():
    print("*** EV_W Iniciado ***")
    print(f"[Config] Conectando a Central en: {CENTRAL_URL}")
    time.sleep(2) 
    
    while True:
        load_cp_locations() 

        print("\n--- Analizando Clima ---")
        for cp_id, city in cp_locations.items():
            
            if cp_id not in cp_weather_state:
                cp_weather_state[cp_id] = "OK"

            temp = get_temperature(city)
            
            # Enviar dato a la central
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
    parser = argparse.ArgumentParser(description='EV Weather Module')
    parser.add_argument('central_host', help='IP o Hostname de la Central (ej: 192.168.1.35)')
    
    args = parser.parse_args()

    # Construimos la URL completa basada en el argumento
    if args.central_host.startswith("http"):
        CENTRAL_URL = args.central_host
    else:
        # Si el usuario solo pone la IP, asumimos puerto 8000 y http
        CENTRAL_URL = f"http://{args.central_host}:8000"

    main()
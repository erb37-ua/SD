# Weather/EV_W.py
import time
import requests
import sys
import os

# --- CONFIGURACIÓN ---
# ¡PON TU API KEY AQUÍ! Si no tienes, el script usará datos falsos si falla la conexión.
OPENWEATHER_API_KEY = "TU_API_KEY_AQUI" 

CENTRAL_URL = os.getenv("CENTRAL_URL", "http://localhost:8000")

# Mapeo de CPs a Ciudades (Puedes editarlo o cargarlo de un fichero)
CP_LOCATIONS = {
    "CP001": "Alicante,ES",
    "CP002": "Madrid,ES",
    "CP003": "Oslo,NO",     # Probablemente haga frío aquí para probar la alerta
    "CP004": "Sevilla,ES"
}

# Estado interno para no spammear a la Central
# { "CP001": "OK", "CP003": "BAD" }
cp_weather_state = {} 

def get_temperature(city):
    """
    Consulta la API de OpenWeatherMap.
    Retorna la temperatura en Celsius (float).
    Si falla, retorna un valor de prueba (simulación).
    """
    url = f"http://api.openweathermap.org/data/2.5/weather?q={city}&appid={OPENWEATHER_API_KEY}&units=metric"
    try:
        response = requests.get(url, timeout=2)
        if response.status_code == 200:
            data = response.json()
            temp = data['main']['temp']
            print(f"[OpenWeather] {city}: {temp}ºC")
            return temp
        else:
            print(f"[Error API] {city}: {response.status_code}")
    except Exception as e:
        print(f"[Error Conexión] {e}")
    
    # --- MODO SIMULACIÓN (FALLBACK) ---
    # Si no hay internet o API Key, simulamos temperaturas basadas en el nombre
    if "Oslo" in city or "Helsinki" in city:
        return -5.0 # Simular frío extremo
    return 20.0 # Simular buen tiempo

def notify_central(cp_id, action):
    """
    Envía POST /api/alert o /api/resume a la Central
    """
    endpoint = "/api/alert" if action == "STOP" else "/api/resume"
    url = f"{CENTRAL_URL}{endpoint}"
    payload = {
        "cp_id": cp_id,
        "reason": "Bad Weather (< 0ºC)" if action == "STOP" else "Weather Improved"
    }
    
    try:
        r = requests.post(url, json=payload)
        if r.status_code == 200:
            print(f"--> [Central] Notificado {action} para {cp_id}")
            return True
        else:
            print(f"xx> [Central] Error {r.status_code}: {r.text}")
    except Exception as e:
        print(f"xx> [Central] Error de conexión: {e}")
    return False

def main():
    print(f"*** EV_W (Weather Office) Iniciado ***")
    print(f"Monitorizando: {list(CP_LOCATIONS.keys())}")
    print(f"Central en: {CENTRAL_URL}")
    
    # Inicializar estado asumido como OK
    for cp in CP_LOCATIONS:
        cp_weather_state[cp] = "OK"

    while True:
        print("\n--- Comprobando Clima ---")
        
        for cp_id, city in CP_LOCATIONS.items():
            temp = get_temperature(city)
            
            # Lógica de Control
            current_status = cp_weather_state[cp_id]
            
            # CASO 1: Hace frío y estaba OK -> ALERTA (STOP)
            if temp < 0 and current_status == "OK":
                print(f"[ALERTA] {city} ({temp}ºC) está helada. Deteniendo {cp_id}...")
                if notify_central(cp_id, "STOP"):
                    cp_weather_state[cp_id] = "BAD"
            
            # CASO 2: Hace bueno y estaba BAD -> RESUME
            elif temp >= 0 and current_status == "BAD":
                print(f"[MEJORA] {city} ({temp}ºC) operativa. Reanudando {cp_id}...")
                if notify_central(cp_id, "RESUME"):
                    cp_weather_state[cp_id] = "OK"
                    
            # CASO 3: Sin cambios
            else:
                status_str = "OPERATIVO" if temp >= 0 else "DETENIDO"
                # print(f"   {cp_id}: {status_str} ({temp}ºC)")

        time.sleep(4) # Esperar 4 segundos según especificación

if __name__ == "__main__":
    main()
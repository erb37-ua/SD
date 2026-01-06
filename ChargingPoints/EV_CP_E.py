import socket
import sys
import threading
import time
import json
import random
from confluent_kafka import Producer, Consumer

# --- ESTADO GLOBAL ---
cp_id_global = None
kafka_producer = None

# Variables de Estado del CP
is_faulty = False           # Avería simulada
is_charging = False         # ¿Está cargando alguien?
is_stopped_admin = False    # Parada administrativa

# Control de Carga
current_charge_stop_event = None
current_charging_driver = None # ID del conductor actual (si hay carga)

# Locks
fault_lock = threading.Lock()
charge_lock = threading.Lock()
admin_lock = threading.Lock()

# --- GESTIÓN DE SESIONES (HEARTBEAT) ---
active_sessions = {} # { 'driverId': timestamp }
session_lock = threading.Lock()
HEARTBEAT_TIMEOUT = 5.0

def input_thread():
    """ Simulación de Averías por teclado """
    global is_faulty
    print("Controles: 'a' = AVERÍA, 'r' = REPARAR")
    while True:
        try:
            k = sys.stdin.readline().strip().lower()
            with fault_lock:
                if k == 'a': 
                    is_faulty = True
                    print("[CP] AVERÍA ACTIVADA (Simulada)")
                elif k == 'r': 
                    is_faulty = False
                    print("[CP] REPARADO")
        except: break

def session_watchdog():
    """ 
    Hilo que revisa constantemente si los drivers conectados siguen vivos.
    """
    global current_charging_driver
    print("[Watchdog] Monitor de sesiones iniciado.")
    
    while True:
        time.sleep(1)
        now = time.time()
        to_remove = []

        with session_lock:
            for did, last_seen in active_sessions.items():
                if (now - last_seen) > HEARTBEAT_TIMEOUT:
                    print(f"[Watchdog] Driver {did} ha expirado (Timeout).")
                    to_remove.append(did)
            
            # Eliminar sesiones muertas
            for did in to_remove:
                del active_sessions[did]
                
                # SI EL QUE MURIÓ ESTÁ CARGANDO, PARAMOS LA CARGA
                with charge_lock:
                    if is_charging and current_charging_driver == did:
                        print(f"[Watchdog] ¡EL DRIVER ACTIVO ({did}) SE HA DESCONECTADO! DETENIENDO CARGA.")
                        # Activamos el evento de parada del hilo de carga
                        if current_charge_stop_event:
                            current_charge_stop_event.set()

def charging_thread(driver_id, stop_evt):
    """ Lógica de carga pura (ya no vigila el heartbeat, el Watchdog lo hace) """
    global is_charging, is_faulty, current_charging_driver
    
    with charge_lock:
        is_charging = True
        current_charging_driver = driver_id

    consumo = 0.0
    coste = 0.0
    razon_fin = "COMPLETED"
    status_fin = "COMPLETED"

    print(f"[Carga] Iniciando suministro para {driver_id}...")

    while not stop_evt.is_set():
        # 1. Chequeo de Avería local
        with fault_lock:
            if is_faulty:
                print("[Carga] Error Crítico: Avería interna.")
                razon_fin = "Technical Fault"
                status_fin = "ERROR"
                break
        
        # 2. Simulación
        time.sleep(1)
        if stop_evt.is_set(): # Puede ser activado por STOP manual o por WATCHDOG
            print("[Carga] Detenida por señal externa (Usuario, Central o Timeout).")
            # Si fue por timeout (verificado externamente), el status sigue siendo completed o error?
            # Asumiremos que si se para abruptamente es una desconexión, pero si es manual es ok.
            # Para simplificar, si no hubo fallo técnico, es COMPLETED (se cobra lo cargado).
            break

        consumo += random.uniform(0.1, 0.5)
        coste = consumo * 0.50

        # Telemetría
        msg = {
            'cpId': cp_id_global, 'driverId': driver_id, 
            'status': 'CHARGING', 'consumo_kw': round(consumo,3), 'importe_eur': round(coste,2)
        }
        send_kafka('telemetry', msg)

    # Fin del proceso
    final_msg = {
        'cpId': cp_id_global, 'driverId': driver_id,
        'status': status_fin, 'reason': razon_fin,
        'final_consumo_kw': round(consumo,3), 'final_importe_eur': round(coste,2)
    }
    
    # Enviar Ticket
    send_kafka('tickets', final_msg)
    # Si fue error, enviar también telemetría final de error
    if status_fin == "ERROR":
        send_kafka('telemetry', final_msg)

    with charge_lock:
        is_charging = False
        current_charging_driver = None

def kafka_consumer_loop(broker, cp_id):
    """ Procesa comandos y actualiza sesiones """
    c = Consumer({'bootstrap.servers': broker, 'group.id': f'cp_{cp_id}', 'auto.offset.reset': 'latest'})
    c.subscribe(['commands'])
    
    while True:
        msg = c.poll(1.0)
        if not msg or msg.error(): continue
        
        try:
            data = json.loads(msg.value().decode('utf-8'))
            if data.get('cpId') != cp_id: continue
            
            did = data.get('driverId')
            action = data.get('action')

            # 1. GESTIÓN DE HEARTBEAT (PRIORIDAD)
            if action == 'HEARTBEAT':
                with session_lock:
                    active_sessions[did] = time.time()
                continue # No procesamos más

            # 2. COMANDOS
            if action == 'AUTHORIZE':
                with charge_lock:
                    if is_charging: continue # Ocupado
                with admin_lock:
                    if is_stopped_admin: continue # Bloqueado
                
                # Iniciar carga
                evt = threading.Event()
                global current_charge_stop_event
                current_charge_stop_event = evt
                threading.Thread(target=charging_thread, args=(did, evt)).start()

            elif action == 'STOP':
                # Parada manual o central
                if current_charge_stop_event:
                    current_charge_stop_event.set()

        except Exception: pass

def send_kafka(topic, data):
    try:
        kafka_producer.produce(topic, value=json.dumps(data).encode('utf-8'))
        kafka_producer.poll(0)
    except: pass

def main():
    global kafka_producer, cp_id_global
    if len(sys.argv) < 4: return
    
    port = int(sys.argv[1])
    broker = sys.argv[2]
    cp_id_global = sys.argv[3]

    kafka_producer = Producer({'bootstrap.servers': broker})

    # Iniciar hilos
    threading.Thread(target=input_thread, daemon=True).start()
    threading.Thread(target=kafka_consumer_loop, args=(broker, cp_id_global), daemon=True).start()
    threading.Thread(target=session_watchdog, daemon=True).start() # NUEVO: Hilo Watchdog independiente

    # Socket Monitor (Solo Health Check básico)
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(('0.0.0.0', port))
    srv.listen(1)
    print(f"*** CP {cp_id_global} ONLINE ***")
    
    while True:
        try:
            conn, _ = srv.accept()
            with conn:
                while True:
                    d = conn.recv(1024)
                    if not d: break
                    if d.decode().strip() == "HEALTH_CHECK":
                        with fault_lock:
                            st = "KO" if is_faulty else "OK"
                        conn.sendall(st.encode())
        except: break

if __name__ == "__main__":
    main()
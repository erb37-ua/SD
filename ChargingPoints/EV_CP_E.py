import socket
import sys
import threading
import time
import json
import random
from confluent_kafka import Producer, Consumer

# --- CONFIGURACIÓN ---
HEARTBEAT_TIMEOUT = 3.0  # Si en 3 seg no hay latido, asumimos que el driver cerró la ventana

# --- ESTADO GLOBAL ---
cp_id_global = None
kafka_producer = None

# Variables de Estado
is_faulty = False           
is_charging = False         
is_stopped_admin = False    

# Control de Carga
current_charge_stop_event = None
current_charging_driver = None 

# Locks para thread-safety
fault_lock = threading.Lock()
charge_lock = threading.Lock()
admin_lock = threading.Lock()

# --- GESTIÓN DE SESIONES (Watchdog) ---
active_sessions = {} # { 'driverId': timestamp }
session_lock = threading.Lock()

def input_thread():
    """ Permite simular averías con el teclado en la terminal del CP """
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
    HILO GUARDIÁN:
    Revisa cada segundo si el driver sigue ahí. Si cierras la terminal del driver,
    este hilo se da cuenta de que falta el latido y corta la carga.
    """
    global current_charging_driver
    print("[Watchdog] Monitor de desconexiones activo.")
    
    while True:
        time.sleep(1)
        now = time.time()
        to_remove = []

        with session_lock:
            # Buscar conductores que han dejado de "latir"
            for did, last_seen in active_sessions.items():
                if (now - last_seen) > HEARTBEAT_TIMEOUT:
                    print(f"[Watchdog] ¡ALERTA! Driver {did} desapareció (Cierre de terminal detectado).")
                    to_remove.append(did)
            
            # Limpiar sesiones muertas y detener cargas si corresponde
            for did in to_remove:
                del active_sessions[did]
                
                # SI EL CONDUCTOR QUE DESAPARECIÓ ESTÁ CARGANDO, PARAMOS TODO
                with charge_lock:
                    if is_charging and current_charging_driver == did:
                        print(f"[Watchdog] Deteniendo carga de {did} para liberar el CP...")
                        if current_charge_stop_event:
                            current_charge_stop_event.set() # Esto despierta al hilo de carga

def charging_thread(driver_id, stop_evt):
    """ 
    Simula la carga. Si 'stop_evt' se activa (por el Watchdog o manualmente),
    termina, envía el ticket y libera el CP.
    """
    global is_charging, is_faulty, current_charging_driver
    
    with charge_lock:
        is_charging = True
        current_charging_driver = driver_id

    consumo = 0.0
    coste = 0.0
    status_fin = "COMPLETED"
    razon_fin = "Finished"

    print(f"[Carga] Suministrando energía a {driver_id}...")

    # Bucle de carga
    while not stop_evt.is_set():
        # 1. Chequeo de Avería interna
        with fault_lock:
            if is_faulty:
                print("[Carga] Abortada por AVERÍA del CP.")
                status_fin = "ERROR"
                razon_fin = "Technical Fault"
                break
        
        # 2. Espera inteligente (Wait) en vez de Sleep
        # Si el Watchdog activa el evento, el wait retorna True INMEDIATAMENTE.
        if stop_evt.wait(timeout=1.0):
            print("[Carga] Interrumpida (Driver desconectado o parada manual).")
            break

        # 3. Simulación
        consumo += random.uniform(0.1, 0.5)
        coste = consumo * 0.50

        # Telemetría
        msg = {
            'cpId': cp_id_global, 'driverId': driver_id, 
            'status': 'CHARGING', 'consumo_kw': round(consumo,3), 'importe_eur': round(coste,2)
        }
        send_kafka('telemetry', msg)

    # --- CIERRE DE CARGA ---
    final_msg = {
        'cpId': cp_id_global, 'driverId': driver_id,
        'status': status_fin, 'reason': razon_fin,
        'final_consumo_kw': round(consumo,3), 'final_importe_eur': round(coste,2)
    }
    
    # IMPORTANTE: Enviamos el Ticket. 
    # Al recibir esto, la Central marcará el CP como "Disponible" (Activado) de nuevo.
    send_kafka('tickets', final_msg)
    
    if status_fin == "ERROR":
        send_kafka('telemetry', final_msg)

    # Liberar variables internas
    with charge_lock:
        is_charging = False
        current_charging_driver = None
    
    print(f"[Carga] Finalizada. CP {cp_id_global} queda LIBRE (Activado).")

def kafka_consumer_loop(broker, cp_id):
    """ Escucha Heartbeats y Comandos """
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

            # ACTUALIZAR LATIDO (Vital para evitar que el Watchdog salte)
            if action == 'HEARTBEAT':
                with session_lock:
                    active_sessions[did] = time.time()
                continue 

            # PROCESAR AUTORIZACIÓN
            if action == 'AUTHORIZE':
                with charge_lock:
                    if is_charging: continue # Ya está ocupado
                with admin_lock:
                    if is_stopped_admin: continue # Está bloqueado
                
                # Iniciar nuevo hilo de carga
                evt = threading.Event()
                global current_charge_stop_event
                current_charge_stop_event = evt
                threading.Thread(target=charging_thread, args=(did, evt)).start()

            # PROCESAR PARADA MANUAL
            elif action == 'STOP':
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

    # Iniciamos los hilos auxiliares
    threading.Thread(target=input_thread, daemon=True).start()
    threading.Thread(target=kafka_consumer_loop, args=(broker, cp_id_global), daemon=True).start()
    threading.Thread(target=session_watchdog, daemon=True).start() # <--- EL WATCHDOG

    # Servidor para Monitor
    srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    srv.bind(('0.0.0.0', port))
    srv.listen(1)
    print(f"*** CP {cp_id_global} ONLINE y LISTO ***")
    
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
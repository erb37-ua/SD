import socket
import sys
import threading
import time
import json
import random
from confluent_kafka import Producer, Consumer
from cryptography.fernet import Fernet

HEARTBEAT_TIMEOUT = 3.0 

cp_id_global = None
kafka_producer = None
aes_key = None

is_faulty = False           
is_charging = False         
is_stopped_admin = False    

current_charge_stop_event = None
current_charging_driver = None 

fault_lock = threading.Lock()
charge_lock = threading.Lock()
admin_lock = threading.Lock()

active_sessions = {} 
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
            for did, last_seen in active_sessions.items():
                if (now - last_seen) > HEARTBEAT_TIMEOUT:
                    print(f"[Watchdog] ¡ALERTA! Driver {did} desapareció (Cierre de terminal detectado).")
                    to_remove.append(did)
            
            for did in to_remove:
                del active_sessions[did]
                
                with charge_lock:
                    if is_charging and current_charging_driver == did:
                        print(f"[Watchdog] Deteniendo carga de {did} para liberar el CP...")
                        if current_charge_stop_event:
                            current_charge_stop_event.set()

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

    while not stop_evt.is_set():
        with fault_lock:
            if is_faulty:
                print("[Carga] Abortada por AVERÍA del CP.")
                status_fin = "ERROR"
                razon_fin = "Technical Fault"
                break
        
        if stop_evt.wait(timeout=1.0):
            print("[Carga] Interrumpida (Driver desconectado o parada manual).")
            break

        consumo += random.uniform(0.1, 0.5)
        coste = consumo * 0.50

        msg = {
            'cpId': cp_id_global, 'driverId': driver_id, 
            'status': 'CHARGING', 'consumo_kw': round(consumo,3), 'importe_eur': round(coste,2)
        }
        send_kafka('telemetry', msg)

    final_msg = {
        'cpId': cp_id_global, 'driverId': driver_id,
        'status': status_fin, 'reason': razon_fin,
        'final_consumo_kw': round(consumo,3), 'final_importe_eur': round(coste,2)
    }
    
    send_kafka('tickets', final_msg)
    
    if status_fin == "ERROR":
        send_kafka('telemetry', final_msg)

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

            if action == 'HEARTBEAT':
                with session_lock:
                    active_sessions[did] = time.time()
                continue 

            if action == 'AUTHORIZE':
                with charge_lock:
                    if is_charging: continue 
                with admin_lock:
                    if is_stopped_admin: continue 
                
                evt = threading.Event()
                global current_charge_stop_event
                current_charge_stop_event = evt
                threading.Thread(target=charging_thread, args=(did, evt)).start()

            elif action == 'STOP':
                if current_charge_stop_event:
                    current_charge_stop_event.set()

        except Exception: pass

def send_kafka(topic, data):
    try:
        if topic == "telemetry":
            if not aes_key:
                print("[CP] Clave AES no configurada, telemetría descartada.")
                return
            f = Fernet(aes_key)
            ciphertext = f.encrypt(json.dumps(data).encode("utf-8")).decode("utf-8")
            envelope = {"cpId": data.get("cpId"), "ciphertext": ciphertext}
            kafka_producer.produce(topic, value=json.dumps(envelope).encode("utf-8"))
        else:
            kafka_producer.produce(topic, value=json.dumps(data).encode('utf-8'))
        kafka_producer.poll(0)
    except: pass

def main():
    global kafka_producer, cp_id_global, aes_key
    if len(sys.argv) < 4: return
    
    port = int(sys.argv[1])
    broker = sys.argv[2]
    cp_id_global = sys.argv[3]

    kafka_producer = Producer({'bootstrap.servers': broker})

    threading.Thread(target=input_thread, daemon=True).start()
    threading.Thread(target=kafka_consumer_loop, args=(broker, cp_id_global), daemon=True).start()
    threading.Thread(target=session_watchdog, daemon=True).start()

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
                    message = d.decode().strip()
                    if message.startswith("SET_KEY#"):
                        key_value = message.split("#", 1)[1]
                        aes_key = key_value.encode("utf-8")
                        conn.sendall("KEY_OK".encode("utf-8"))
                    elif message == "HEALTH_CHECK":
                        with fault_lock:
                            st = "KO" if is_faulty else "OK"
                        conn.sendall(st.encode())
        except: break

if __name__ == "__main__":
    main()

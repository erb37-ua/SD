import socket
import sys
import threading
import time
import json
import random
from confluent_kafka import Producer, Consumer, KafkaError
import uuid

# Estado compartido 
is_faulty = False
fault_lock = threading.Lock()

is_charging = False
charge_lock = threading.Lock()

is_stopped_by_central = False
admin_stop_lock = threading.Lock()

current_charge_stop_event = None
current_charge_lock = threading.Lock()

# --- NUEVO: Control de Heartbeat ---
last_heartbeat_time = 0.0
heartbeat_lock = threading.Lock()
HEARTBEAT_TIMEOUT = 5.0 # Segundos antes de considerar desconexión

kafka_producer = None
cp_id_global = None

def input_thread():
    global is_faulty
    print("Pulsa 'a' para simular AVERÍA, 'r' para REPARAR.")
    while True:
        try:
            key = sys.stdin.readline().strip()
            with fault_lock:
                if key.lower() == 'a':
                    is_faulty = True
                    print("[Simulación] ¡AVERÍA SIMULADA!")
                elif key.lower() == 'r':
                    is_faulty = False
                    print("[Simulación] ¡REPARACIÓN SIMULADA!")
        except EOFError:
            break

def charging_simulation_thread(driver_id, stop_evt: threading.Event):
    global is_faulty, is_charging, last_heartbeat_time
    
    with charge_lock:
        is_charging = True
    
    # Inicializar el timer del heartbeat al inicio de la carga
    with heartbeat_lock:
        last_heartbeat_time = time.time()

    consumo_total_kw = 0.0
    importe_total_eur = 0.0
    precio_kwh = 0.50
    
    print(f"[Simulación] Carga iniciada para {driver_id}. Esperando Heartbeats...")

    exit_reason = "COMPLETED" # Default
    final_status = "COMPLETED"

    while not stop_evt.is_set():
        
        # 1. Comprobar Avería del CP
        with fault_lock:
            if is_faulty:
                print("[Simulación] ¡AVERÍA detectada! Abortando carga.")
                exit_reason = "Technical Fault"
                final_status = "ERROR"
                break 

        # 2. Comprobar Heartbeat del Driver (Watchdog)
        with heartbeat_lock:
            time_since_last_beat = time.time() - last_heartbeat_time
        
        if time_since_last_beat > HEARTBEAT_TIMEOUT:
            print(f"[Simulación] ¡TIMEOUT Heartbeat! ({round(time_since_last_beat,1)}s). Driver desconectado.")
            exit_reason = "Driver Disconnected (Heartbeat Timeout)"
            final_status = "ERROR" # Opcional: Podría ser COMPLETED si queremos cobrar lo cargado hasta ahora
            break

        time.sleep(1)
        
        if stop_evt.is_set():
            print("[Simulación] Parada solicitada.")
            break

        # Simular consumo
        consumo_seg = random.uniform(0.5, 2.0)
        consumo_total_kw += consumo_seg
        importe_total_eur += consumo_seg * precio_kwh
        
        # Telemetría
        telemetry_data = {
            'cpId': cp_id_global,
            'driverId': driver_id,
            'status': 'CHARGING' if not is_faulty else 'ERROR',
            'consumo_kw': round(consumo_total_kw, 3),
            'importe_eur': round(importe_total_eur, 2)
        }
        send_kafka_message('telemetry', telemetry_data)

    # --- FIN DE CARGA ---
    
    print(f"[Simulación] Fin de sesión. Razón: {exit_reason}")
    
    final_msg = {
        'cpId': cp_id_global,
        'driverId': driver_id,
        'status': final_status,
        'reason': exit_reason,
        'final_consumo_kw': round(consumo_total_kw, 3),
        'final_importe_eur': round(importe_total_eur, 2)
    }
    
    if final_status == "COMPLETED":
        # Generamos ticket
        send_kafka_message('tickets', final_msg) # Enviar ticket final
    else:
        # Avisamos del error vía telemetría final o ticket de error
        send_kafka_message('telemetry', final_msg) # Aviso inmediato
        send_kafka_message('tickets', final_msg)   # Ticket de cierre (aunque sea error)

    with charge_lock:
        is_charging = False
    
    with current_charge_lock:
        global current_charge_stop_event
        current_charge_stop_event = None


def kafka_commands_consumer(broker, cp_id):
    """ Escucha comandos del Driver y Central """
    global is_charging, is_stopped_by_central, current_charge_stop_event, last_heartbeat_time
    
    while True:
        consumer = None
        try:
            consumer = Consumer({
                'bootstrap.servers': broker,
                'group.id': f'cp_group_{cp_id}_{uuid.uuid4()}',
                'auto.offset.reset': 'latest'
            })
            consumer.subscribe(['commands'])
            print(f"[Kafka] Consumidor 'commands' listo.")

            while True:
                msg = consumer.poll(1.0)
                if msg is None: continue
                if msg.error(): continue
                
                try:
                    cmd = json.loads(msg.value().decode('utf-8'))
                    if cmd.get('cpId') != cp_id: continue 

                    action = cmd.get('action')

                    # --- GESTIÓN DE HEARTBEAT ---
                    if action == 'HEARTBEAT':
                        with heartbeat_lock:
                            last_heartbeat_time = time.time()
                        # No imprimimos nada para no saturar la consola
                        continue

                    # --- GESTIÓN DE OTROS COMANDOS ---
                    driver_id = cmd.get('driverId')

                    if action == 'AUTHORIZE':
                        with admin_stop_lock:
                            if is_stopped_by_central:
                                print("Rechazado: CP Bloqueado.")
                                continue
                        
                        with charge_lock:
                            if not is_charging:
                                print(f"Autorizado {driver_id}. Iniciando...")
                                stop_evt = threading.Event()
                                with current_charge_lock:
                                    current_charge_stop_event = stop_evt
                                
                                threading.Thread(
                                    target=charging_simulation_thread, 
                                    args=(driver_id, stop_evt)
                                ).start()
                            else:
                                print("Ocupado.")
                    
                    elif action == 'STOP':
                        print(f"Orden de STOP recibida.")
                        with current_charge_lock:
                            if current_charge_stop_event:
                                current_charge_stop_event.set()

                except json.JSONDecodeError:
                    pass
        
        except Exception as e:
            print(f"Error Kafka Consumer: {e}")
            time.sleep(5)

def send_kafka_message(topic, message_data):
    global kafka_producer
    try:
        kafka_producer.produce(topic, value=json.dumps(message_data).encode('utf-8'))
        kafka_producer.poll(0)
    except Exception as e:
        print(f"Error envio Kafka: {e}")

def main():
    global kafka_producer, cp_id_global

    if len(sys.argv) < 4:
        print("Uso: python EV_CP_E.py <port> <broker> <cp_id>")
        return

    listen_port = int(sys.argv[1])
    kafka_broker = sys.argv[2]
    cp_id_global = sys.argv[3]

    kafka_producer = Producer({'bootstrap.servers': kafka_broker})

    threading.Thread(target=input_thread, daemon=True).start()
    threading.Thread(
        target=kafka_commands_consumer, args=(kafka_broker, cp_id_global), daemon=True
    ).start()

    # Servidor socket para Monitor (sin cambios lógica principal)
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server.bind(('0.0.0.0', listen_port))
        server.listen(1)
        print(f"*** CP Engine {cp_id_global} OK. Puerto {listen_port} ***")
        
        conn, _ = server.accept()
        with conn:
            while True:
                data = conn.recv(1024)
                if not data: break
                if data.decode('utf-8') == "HEALTH_CHECK":
                    with fault_lock:
                        res = "KO" if is_faulty else "OK"
                    conn.sendall(res.encode('utf-8'))
    except KeyboardInterrupt:
        pass
    finally:
        kafka_producer.flush()
        server.close()

if __name__ == "__main__":
    main()
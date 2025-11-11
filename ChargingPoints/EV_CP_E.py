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

# Estado de recarga
is_charging = False
charge_lock = threading.Lock()

# --- NUEVO: Estado de parada administrativa ---
is_stopped_by_central = False
admin_stop_lock = threading.Lock()

# --- NUEVO: Evento para interrumpir una recarga en curso ---
current_charge_stop_event = None
current_charge_lock = threading.Lock()

# Kafka Producer Global
kafka_producer = None
cp_id_global = None # ID de este CP

# Hilo de Input
def input_thread():
    """
    Un hilo dedicado a escuchar la entrada del teclado para simular
    una avería o una recuperación.
    """
    global is_faulty
    print("Pulsa 'a' para simular una AVERÍA.")
    print("Pulsa 'r' para simular una REPARACIÓN.")
    
    while True:
        try:
            key = sys.stdin.readline().strip()
            
            with fault_lock:
                if key.lower() == 'a':
                    is_faulty = True
                    print("[Simulación] ¡AVERÍA SIMULADA! Se reportará 'KO'.")
                elif key.lower() == 'r':
                    is_faulty = False
                    print("[Simulación] ¡REPARACIÓN SIMULADA! Se reportará 'OK'.")

        except EOFError:
            break

# Hilo de Simulación de Recarga
def charging_simulation_thread(driver_id, stop_evt: threading.Event):
    """
    Simula una recarga de 10 segundos, enviando telemetría
    """
    global is_faulty, is_charging, is_stopped_by_central
    
    # Aseguramos que el estado de recarga esté activo
    with charge_lock:
        is_charging = True
    
    consumo_total_kw = 0.0
    importe_total_eur = 0.0
    precio_kwh = 0.50
    
    print(f"[Simulación] Iniciando recarga para {driver_id}...")

    charge_interrupted = False

    for i in range(10): # Simular 10 segundos
        if stop_evt.is_set():
            print("[Simulación] Recarga INTERRUMPIDA por comando STOP (Driver o Admin).")
            charge_interrupted = True
            break

        time.sleep(1) 
        
        with fault_lock:
            if is_faulty:
                print("[Simulación] ¡AVERÍA durante la recarga! Interrumpiendo.")
                charge_interrupted = True
                break 
        
        # Simular consumo
        consumo_seg = random.uniform(0.5, 2.0)
        importe_seg = consumo_seg * precio_kwh
        consumo_total_kw += consumo_seg
        importe_total_eur += importe_seg
        
        # Enviar telemetría de recarga
        telemetry_data = {
            'cpId': cp_id_global,
            'driverId': driver_id,
            'status': 'CHARGING',
            'consumo_kw': round(consumo_total_kw, 3),
            'importe_eur': round(importe_total_eur, 2)
        }
        send_kafka_message('telemetry', telemetry_data)
        print(f"[Simulación] Telemetría {i+1}/10 enviada.")

    if not charge_interrupted:
        # Simulación finalizada (normalmente)
        print(f"[Simulación] Recarga finalizada para {driver_id}.")
        completion_data = {
            'cpId': cp_id_global,
            'driverId': driver_id,
            'status': 'COMPLETED',
            'final_consumo_kw': round(consumo_total_kw, 3),
            'final_importe_eur': round(importe_total_eur, 2)
        }
        send_kafka_message('telemetry', completion_data)
    else:
        # Si fue interrumpida, no se envía ticket.
        print(f"[Simulación] Recarga para {driver_id} parada sin ticket.")

    # Resetear estado
    with charge_lock:
        is_charging = False
    
    # Limpiar el evento de parada global
    with current_charge_lock:
        global current_charge_stop_event
        current_charge_stop_event = None

# Hilo consumidor de Kafka
def kafka_commands_consumer(broker, cp_id):
    """
    Hilo consumidor para 'commands'
    """
    global is_charging, is_stopped_by_central, current_charge_stop_event
    print(f"[Kafka] Hilo consumidor de 'commands' para {cp_id} iniciado.")

    while True:
        consumer = None
        try:
            consumer_config = {
                'bootstrap.servers': broker,
                'group.id': f'cp_group_{cp_id}_{uuid.uuid4()}',
                'auto.offset.reset': 'earliest'
            }
            consumer = Consumer(consumer_config)
            consumer.subscribe(['commands'])
            print(f"[Kafka] Consumidor de 'commands' ({cp_id}) suscrito.")

            while True:
                msg = consumer.poll(1.0)
                if msg is None: continue
                
                if msg.error():
                    err_code = msg.error().code()
                    if err_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        print(f"[{cp_id}] Topic 'commands' no encontrado, re-intentando subscripción...")
                        break
                    else:
                        print(f"[Error Kafka Commands] {msg.error()}")
                        continue
                
                # Mensaje válido
                try:
                    command = json.loads(msg.value().decode('utf-8'))
                    
                    # Ignorar comandos para otros CPs
                    if command.get('cpId') != cp_id:
                        continue 

                    # --- LÓGICA DE COMANDOS MODIFICADA ---
                    action = command.get('action')
                    driver_id = command.get('driverId') # Puede ser None

                    if action == 'AUTHORIZE':
                        print(f"\n[Kafka Command] Recarga AUTORIZADA para {driver_id}.")
                        
                        # Comprobar si está parado por Admin
                        with admin_stop_lock:
                            if is_stopped_by_central:
                                print(f"[Simulación] RECHAZADO: CP está en PARADA (Admin).")
                                # (Opcional: enviar ticket de rechazo a 'tickets')
                                continue
                        
                        # Comprobar si ya está cargando
                        with charge_lock:
                            if not is_charging:
                                print("[Simulación] 'Enchufado' automático en 1 segundo...")
                                time.sleep(1)
                                
                                # Crear y guardar el evento de parada para esta recarga
                                stop_evt = threading.Event()
                                with current_charge_lock:
                                    current_charge_stop_event = stop_evt
                                
                                # Iniciar hilo de simulación
                                threading.Thread(
                                    target=charging_simulation_thread, 
                                    args=(driver_id, stop_evt) # Pasar el evento
                                ).start()
                            else:
                                print(f"\n[Kafka Command] Autorización recibida, pero ya hay una carga en curso.")
                    
                    elif action == 'STOP':
                        if driver_id:
                            # Parada de un Driver
                            print(f"\n[Kafka Command] Petición de PARADA (Driver: {driver_id}) recibida.")
                        else:
                            # Parada de la CENTRAL (Admin)
                            print(f"\n[Kafka Command] Petición de PARADA (Admin) recibida.")
                            with admin_stop_lock:
                                is_stopped_by_central = True # Poner en estado Parado
                        
                        # Interrumpir la recarga actual (si hay una)
                        with current_charge_lock:
                            if current_charge_stop_event:
                                print("[Simulación] Interrumpiendo recarga en curso...")
                                current_charge_stop_event.set()
                            else:
                                print("[Simulación] Petición de PARADA recibida, pero no hay carga activa.")

                    elif action == 'RESUME':
                        # Reanudación de la CENTRAL (Admin)
                        print(f"\n[Kafka Command] Petición de REANUDAR (Admin) recibida.")
                        with admin_stop_lock:
                            is_stopped_by_central = False # Quitar estado Parado
                    # --- FIN DE LÓGICA DE COMANDOS ---

                except json.JSONDecodeError:
                    print("[Error Kafka Commands] Mensaje con JSON inválido.")
        
        except Exception as e:
            print(f"[Error] Excepción grave en consumidor de 'commands': {e}")
        finally:
            if consumer:
                consumer.close()
            print(f"[Kafka] Consumidor 'commands' ({cp_id}) cerrado, re-intentando en 3s...")
            time.sleep(3)


def send_kafka_message(topic, message_data):
    global kafka_producer
    try:
        payload = json.dumps(message_data).encode('utf-8')
        kafka_producer.produce(topic, value=payload)
        kafka_producer.poll(0)
    except Exception as e:
        print(f"[Error Kafka Produce] No se pudo enviar a {topic}: {e}")


def main():
    global is_faulty, kafka_producer, cp_id_global

    if len(sys.argv) < 4:
        print("Error: Faltan argumentos.")
        print("Uso: python EV_CP_E.py <puerto_socket> <kafka_broker_host:port> <cp_id>")
        return

    try:
        listen_port = int(sys.argv[1])
        kafka_broker = sys.argv[2]
        cp_id_global = sys.argv[3]
    except ValueError:
        print("Error: El puerto debe ser un número entero.")
        return
    except Exception as e:
        print(f"Error parseando argumentos: {e}")
        return

    try:
        kafka_producer = Producer({'bootstrap.servers': kafka_broker})
        print(f"[Kafka] Productor conectado a {kafka_broker}")
    except Exception as e:
        print(f"[Error Kafka] No se pudo conectar el Productor: {e}")
        return

    threading.Thread(target=input_thread, daemon=True).start()
    threading.Thread(
        target=kafka_commands_consumer, args=(kafka_broker, cp_id_global), daemon=True
    ).start()

    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_socket.bind(('0.0.0.0', listen_port))
        server_socket.listen(1)
        print(f"*** EV_CP_E (Engine) [{cp_id_global}] iniciado ***")
        print(f"Escuchando al Monitor en el puerto {listen_port}...")

        conn, addr = server_socket.accept()
        print(f"[Conexión] Monitor conectado desde: {addr[0]}:{addr[1]}")

        with conn:
            while True:
                data = conn.recv(1024)
                if not data:
                    print("[Desconexión] El Monitor se ha desconectado.")
                    break
                
                message = data.decode('utf-8')
                if message == "HEALTH_CHECK":
                    with fault_lock:
                        response = "KO" if is_faulty else "OK"
                    conn.sendall(response.encode('utf-8'))
                
    except socket.error as e:
        print(f"Error de Socket: {e}")
    except KeyboardInterrupt:
        print("\nCerrando Engine... ¡Adiós!")
    finally:
        kafka_producer.flush()
        server_socket.close()

if __name__ == "__main__":
    main()
# Importaciones estándar para la concurrencia, sockets, argumentos y refresco de pantalla
import socket
import sys
import threading
import time
import os
# Importaciones para la comunicación por streaming de eventos con Kafka
import json 
from confluent_kafka import Producer, Consumer, KafkaError
import uuid

# Constantes para colores
VERDE = '\033[92m'
NARANJA = '\033[93m'
ROJO = '\033[91m'
GRIS = '\033[90m'
RESET = '\033[0m'

# Almacenamiento del estado de los Puntos de Recarga (CPs)
charging_points = {}

# Lock para la estructura de datos, previene condiciones de carrera y corrupción de datos
# asegurando la "resiliencia"
db_lock = threading.Lock()

# Variable global del productor Kafka, para poder enviar mensajes de respuesta
kafka_producer = None

def load_database(filename="cp_database.txt"):
    """
    Carga los CPs conocidos desde un archivo de texto al iniciar
    """
    print(f"[Info] Cargando base de datos de CPs desde {filename}...")
    try:
        with open(filename, 'r') as f:
            for line in f:
                if line.strip():
                    parts = line.strip().split(',')
                    if len(parts) >= 3:
                        cp_id = parts[0]
                        location = parts[1]
                        price = float(parts[2])
                        
                        with db_lock:
                            charging_points[cp_id] = {
                                "location": location,
                                "price": price,
                                "state": "DESCONECTADO",
                                "driver": None,
                                "consumo": 0.0,
                                "importe": 0.0
                            }
        # Si todo es correcto
        print(f"[Info] Cargados {len(charging_points)} CPs.")
    except FileNotFoundError:
        print(f"[Error] No se encontró el archivo {filename}. Empezando con 0 CPs.")
    except Exception as e:
        print(f"[Error] Al cargar {filename}: {e}")

def display_panel():
    """
    Un hilo dedicado a refrescar y mostrar el panel de control
    en la terminal cada 2 segundos.
    """
    
    while True:
        print("************************************************************")
        print(f"*** {VERDE}     PANEL DE MONOTORIZACIÓN         {RESET} ***")
        print("************************************************************")
        
        with db_lock:
            if not charging_points:
                print("No hay Puntos de Recarga (CPs) registrados en la BD.")
            
            cp_ids = sorted(charging_points.keys())

            for cp_id in cp_ids:
                cp = charging_points[cp_id]
                state = cp['state']
                color = GRIS

                if state == "Activado":
                    color = VERDE
                elif state == "Suministrando":
                    color = VERDE
                elif state == "Averiado":
                    color = ROJO
                elif state == "Parado":
                    color = NARANJA

                print(f"{color}--- CP: {cp_id} ---{RESET}")
                print(f"{color}| Ubicación: {cp['location']:<20} {RESET}")
                print(f"{color}| Estado:    {state:<20} {RESET}")
                
                if state == "Suministrando":
                    print(f"{color}| Driver:    {cp['driver']:<20} {RESET}")
                    print(f"{color}| Consumo:   {cp['consumo']:.2f} kWh{RESET}")
                    print(f"{color}| Importe:   {cp['importe']:.2f} €{RESET}")
                print(f"{color}--------------------{RESET}\n")

        print("--- APLICATION MESSAGES ---")
        print("CENTRAL system status OK")
        
        # Refrescar cada dos segundos
        time.sleep(2)

def handle_client(conn, addr):
    """
    Esta función se ejecuta en su propio hilo por cada cliente
    que se conecta al socket del servidor
    """
    print(f"\n[Nueva Conexión] Recibida conexión de: {addr[0]}:{addr[1]}")

    cp_id_conectado = None

    try:
        while True:
            data = conn.recv(1024)
            if not data:
                print(f"[Desconexión] Cliente {addr[0]}:{addr[1]} se ha desconectado.")
                break
            
            message = data.decode('utf-8')
            parts = message.split('#')
            response = "ACK"

            if parts[0] == "REGISTER" and len(parts) >= 3:
                cp_id = parts[1]
                location = parts[2]
                cp_id_conectado = cp_id
                
                with db_lock:
                    if cp_id in charging_points:
                        charging_points[cp_id]['state'] = "Activado"
                        print(f"[Registro] CP '{cp_id}' se ha activado.")
                    else:
                        charging_points[cp_id] = {
                            "location": location, "price": 0.50, "state": "Activado",
                            "driver": None, "consumo": 0.0, "importe": 0.0
                        }
                        print(f"[Registro] Nuevo CP '{cp_id}' registrado y activado.")
                
                response = "ACK: Registrado y Activado"

            elif parts[0] == "FAULT" and len(parts) >= 2:
                cp_id = parts[1]
                cp_id_conectado = cp_id
                
                with db_lock:
                    if cp_id in charging_points:
                        if charging_points[cp_id]['state'] != "Averiado":
                            charging_points[cp_id]['state'] = "Averiado"
                            print(f"[Avería] CP '{cp_id}' ha reportado una avería.")
                response = "ACK: FAULT Recibido"

            elif parts[0] == "HEALTHY" and len(parts) >= 2:
                cp_id = parts[1]
                cp_id_conectado = cp_id

                with db_lock:
                    if cp_id in charging_points:
                        if charging_points[cp_id]['state'] == "Averiado":
                            charging_points[cp_id]['state'] = "Activado"
                            print(f"[Recuperado] CP '{cp_id}' vuelve a estar Activo.")
                response = "ACK: HEALTHY Recibido"

            elif not parts[0] in ["REGISTER", "FAULT", "HEALTHY"]:
                print(f"[Mensaje] Mensaje no reconocido de {addr[0]}: {message}")
                response = "NACK: Mensaje no reconocido"

            conn.sendall(response.encode('utf-8'))

    except socket.error:
        # Silenciamos los errores de "conexión cerrada" que son normales
        print(f"[Info] Conexión con {addr[0]} perdida o cerrada.")

    # Detecta cuándo un monitor se desconecta y actualiza el estado del CP a "desconectado"
    finally:
        if cp_id_conectado:
            with db_lock:
                if cp_id_conectado in charging_points and charging_points[cp_id_conectado]['state'] == "Activado":
                    charging_points[cp_id_conectado]['state'] = "DESCONECTADO"
                    print(f"[Estado] CP '{cp_id_conectado}' pasa a DESCONECTADO.")
        
        conn.close()
        print(f"[Conexión Cerrada] Hilo para {addr[0]}:{addr[1]} finalizado.")


# FUNCIONES KAFKA

def send_kafka_message(topic, message_data):
    """
    Función de ayuda para enviar mensajes con el productor global
    """
    global kafka_producer
    try:
        payload = json.dumps(message_data).encode('utf-8')
        kafka_producer.produce(topic, value=payload)
        # No bloquear, solo disparar envío
        kafka_producer.poll(0) 

    except Exception as e:
        print(f"[Error Kafka Produce] No se pudo enviar a {topic}: {e}")


def kafka_requests_consumer(broker):
    """ 
    Hilo que escucha permanentemente el topic "requests"
    """

    print("[Kafka] Hilo consumidor de 'requests' iniciado.")
    
    # Bucle exterior: Recrea el consumidor si falla
    while True:
        consumer = None
        try:
            consumer_config = {
                'bootstrap.servers': broker,
                'group.id': f'central_requests_group_{uuid.uuid4()}',
                'auto.offset.reset': 'earliest'
            }
            consumer = Consumer(consumer_config)
            consumer.subscribe(['requests'])
            print("[Kafka] Consumidor de 'requests' suscrito.")

            # Bucle interior: Procesa mensajes
            while True: 
                msg = consumer.poll(1.0)
                if msg is None: continue
                
                if msg.error():
                    err_code = msg.error().code()
                    if err_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        print("[Kafka] Topic 'requests' no encontrado, re-intentando subscripción...")
                        break # Rompe el bucle INTERIOR para recrear el consumidor
                    else:
                        print(f"[Error Kafka Requests] {msg.error()}")
                        continue # Ignora otros errores y sigue

                # Mensaje válido
                try:
                    request = json.loads(msg.value().decode('utf-8'))
                    cp_id = request.get('cpId')
                    driver_id = request.get('driverId')
                    print(f"[Kafka Request] Recibida petición de {driver_id} para {cp_id}")

                    # Lógica de autorización
                    with db_lock:
                        cp = charging_points.get(cp_id)
                        
                        if cp and cp['state'] == 'Activado':
                            auth_message = {'cpId': cp_id, 'driverId': driver_id, 'action': 'AUTHORIZE'}
                            send_kafka_message('commands', auth_message)
                            print(f"[Kafka Command] Autorización enviada a {cp_id} para {driver_id}")
                        else:
                            reason = "CP no disponible o averiado" if cp else "CP desconocido"
                            ticket_message = {'driverId': driver_id, 'cpId': cp_id, 'status': 'REJECTED', 'reason': reason}
                            send_kafka_message('tickets', ticket_message)
                            print(f"[Kafka Ticket] Petición rechazada para {driver_id} ({reason})")
                
                except json.JSONDecodeError:
                    print("[Error Kafka Requests] Mensaje con JSON inválido.")
        
        except Exception as e:
            print(f"[Error] Excepción grave en consumidor de 'requests': {e}")
        finally:
            if consumer:
                consumer.close()
            print("[Kafka] Consumidor 'requests' cerrado, reintentando en 3s...")
            # Espera antes de recrear el consumidor
            time.sleep(3) 

def kafka_telemetry_consumer(broker):
    """ 
    Hilo que escucha permanentemente el topic "telemetry"
    """

    print("[Kafka] Hilo consumidor de 'telemetry' iniciado.")

    while True:
        consumer = None
        try:
            consumer_config = {
                'bootstrap.servers': broker,
                'group.id': f'central_telemetry_group_{uuid.uuid4()}',
                'auto.offset.reset': 'latest'
            }
            consumer = Consumer(consumer_config)
            consumer.subscribe(['telemetry'])
            print("[Kafka] Consumidor de 'telemetry' suscrito.")

            while True:
                msg = consumer.poll(1.0)
                if msg is None: continue
                
                if msg.error():
                    err_code = msg.error().code()
                    if err_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        print("[Kafka] Topic 'telemetry' no encontrado, re-intentando subscripción...")
                        break # Rompe el bucle INTERIOR
                    else:
                        print(f"[Error Kafka Telemetry] {msg.error()}")
                        continue

                # Mensaje válido
                try:
                    telemetry = json.loads(msg.value().decode('utf-8'))
                    cp_id = telemetry.get('cpId')
                    status = telemetry.get('status')

                    with db_lock:
                        if cp_id not in charging_points:
                            continue 

                        if status == 'CHARGING':
                            charging_points[cp_id]['state'] = "Suministrando"
                            charging_points[cp_id]['driver'] = telemetry.get('driverId')
                            charging_points[cp_id]['consumo'] = telemetry.get('consumo_kw')
                            charging_points[cp_id]['importe'] = telemetry.get('importe_eur')
                        
                        elif status == 'COMPLETED':
                            print(f"[Kafka Telemetry] Recibida finalización de {cp_id}")
                            ticket_message = {
                                'driverId': charging_points[cp_id]['driver'],
                                'cpId': cp_id,
                                'status': 'COMPLETED',
                                'final_consumo_kw': telemetry.get('final_consumo_kw'),
                                'final_importe_eur': telemetry.get('final_importe_eur')
                            }
                            send_kafka_message('tickets', ticket_message)
                            
                            charging_points[cp_id]['state'] = "Activado"
                            charging_points[cp_id]['driver'] = None
                            charging_points[cp_id]['consumo'] = 0.0
                            charging_points[cp_id]['importe'] = 0.0

                except json.JSONDecodeError:
                     print("[Error Kafka Telemetry] Mensaje con JSON inválido.")
        
        except Exception as e:
            print(f"[Error] Excepción grave en consumidor de 'telemetry': {e}")
        finally:
            if consumer:
                consumer.close()
            print("[Kafka] Consumidor 'telemetry' cerrado, reintentando en 3s...")
            time.sleep(3)


def main():
    global kafka_producer # Indicar que usaremos la variable global

    # 1. Validar argumentos (AHORA 2)
    if len(sys.argv) < 3:
        print("Error: Debes especificar un puerto y el broker de Kafka.")
        print("Uso: python EV_Central.py <puerto_socket> <kafka_broker>")
        return

    try:
        listen_port = int(sys.argv[1])
        kafka_broker = sys.argv[2]
    except ValueError:
        print("Error: El puerto debe ser un número entero.")
        return

    # 1.b. Cargar la "Base de Datos"
    load_database()

    # Inicializar Kafka Producer
    try:
        kafka_producer = Producer({'bootstrap.servers': kafka_broker})
        print(f"[Kafka] Productor conectado a {kafka_broker}")
    except Exception as e:
        print(f"[Error Kafka] No se pudo conectar el Productor: {e}")
        return

    # 2. Iniciar el hilo del Panel de Control
    panel_thread = threading.Thread(target=display_panel, daemon=True)
    panel_thread.start()

    # Iniciar Hilos Consumidores de Kafka
    req_thread = threading.Thread(
        target=kafka_requests_consumer, args=(kafka_broker,), daemon=True
    )
    req_thread.start()

    tel_thread = threading.Thread(
        target=kafka_telemetry_consumer, args=(kafka_broker,), daemon=True
    )
    tel_thread.start()


    # 3. Crear y configurar el socket del servidor
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        server_socket.bind(('0.0.0.0', listen_port))
        server_socket.listen(5)
        print(f"\n*** EV_Central iniciada (Concurrente + Kafka) ***")
        print(f"Escuchando Sockets en el puerto {listen_port}...")
        print("El panel de control se está mostrando. Esperando conexiones...")

        # 4. Bucle principal para aceptar conexiones de Sockets (Monitores)
        while True:
            conn, addr = server_socket.accept()
            client_thread = threading.Thread(
                target=handle_client, 
                args=(conn, addr)
            )
            client_thread.start()

    except socket.error as e:
        print(f"Error de Socket: {e}")
    except KeyboardInterrupt:
        print("\nCerrando servidor... ¡Adiós!")
    finally:
        kafka_producer.flush() # Vaciar buffer del productor
        server_socket.close()

if __name__ == "__main__":
    main()
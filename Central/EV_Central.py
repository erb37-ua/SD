import sys
import os
import json
import uuid
import socket
import signal
import threading
import asyncio
import time
from confluent_kafka import Producer, Consumer, KafkaError
import uvicorn
from threading import Thread
import jwt
from cryptography.fernet import Fernet, InvalidToken


# Colores
VERDE = '\033[92m'
NARANJA = '\033[38;5;208m'
ROJO = '\033[91m'
GRIS = '\033[90m'
RESET = '\033[0m'

# Estado CPs
charging_points = {}
cp_aes_keys = {}
keys_lock = threading.Lock()

JWT_SECRET = os.getenv("REGISTRY_JWT_SECRET", "dev_registry_secret")
JWT_ALG = "HS256"

# Kafka consumers se ejecutan en hilos
db_lock = threading.Lock()
audit_lock = threading.Lock()

# Kafka producer global
kafka_producer = None

# Eventos para parada limpia
stop_event_threads = threading.Event()   # para hilos
stop_event_async = None                  # será asyncio.Event()

# Estructura para la cola
pending_requests = []

def log_audit(evento, ip, accion):
    timestamp = time.strftime("%Y-%m-%dT%H:%M:%S%z")
    line = f"{timestamp} | evento={evento} | ip={ip} | accion={accion}\n"
    audit_path = os.path.join(os.path.dirname(__file__), "audit.log")
    with audit_lock:
        with open(audit_path, "a", encoding="utf-8") as f:
            f.write(line)

def validate_jwt(token, cp_id):
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALG])
    except jwt.PyJWTError as exc:
        return False, f"jwt_error:{exc}"
    if payload.get("cp_id") != cp_id:
        return False, "cp_id_mismatch"
    return True, payload

def get_state_snapshot():
    """
    Devuelve una copia segura del estado de los CPs para el panel web.
    """
    with db_lock:
        # copia superficial de dicts (suficiente para lectura)
        return {cp_id: dict(data) for cp_id, data in charging_points.items()}

# Funciones de BD / panel
def load_database(filename="cp_database.json"): # <--- Cambiado a .json
    """
    Carga los CPs desde un archivo JSON.
    Estructura esperada: [{"id":"CP001", "location":"...", "city":"...", "price":0.5}, ...]
    """
    print(f"[Info] Cargando base de datos de CPs desde {filename}...")
    try:
        # Aseguramos ruta absoluta si es necesario, o confiamos en el workdir
        if not os.path.exists(filename):
            print(f"[Error] No se encontró {filename}. Empezando con 0 CPs.")
            return

        with open(filename, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
            with db_lock:
                for item in data:
                    cp_id = item.get("id")
                    if cp_id:
                        charging_points[cp_id] = {
                            "location": item.get("location", "Desconocida"),
                            "city": item.get("city", "Alicante"), # Guardamos la ciudad también
                            "price": item.get("price", 0.50),
                            "state": "DESCONECTADO",
                            "driver": None,
                            "consumo": 0.0,
                            "importe": 0.0
                        }
        print(f"[Info] Cargados {len(charging_points)} CPs.")

    except json.JSONDecodeError:
        print(f"[Error] El archivo {filename} no es un JSON válido.")
    except Exception as e:
        print(f"[Error] Al cargar {filename}: {e}")


def save_database(filename="cp_database.json"):
    """
    Guarda el estado actual (incluyendo cambios de ciudad) en el JSON.
    """
    try:
        data_list = []
        with db_lock:
            for cp_id, data in charging_points.items():
                # Reconstruimos el objeto para guardar
                item = {
                    "id": cp_id,
                    "location": data["location"],
                    "city": data.get("city", "Alicante"), # Guardamos la ciudad
                    "price": data["price"]
                }
                data_list.append(item)
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(data_list, f, indent=2)
        print(f"[Info] Base de datos actualizada con nueva ciudad.")
    except Exception as e:
        print(f"[Error] Al guardar base de datos: {e}")


async def display_panel():
    """
    Coroutine que refresca el panel cada 2s
    """
    while not stop_event_async.is_set():
        os.system('cls' if os.name == 'nt' else 'clear')
        print("************************************************************")
        print(f"*** {VERDE}     PANEL DE MONITORIZACIÓN         {RESET} ***")
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
        # esperar 2 segundos
        await asyncio.sleep(2)

# Hilo para leer comandos de administrador (stop/resume)
def admin_input_thread(stop_evt: threading.Event):
    """
    Un hilo separado que bloquea en input() para recibir comandos
    del administrador (ej: 'stop CP001', 'resume CP001').
    """
    print("\n[Admin] Hilo de comandos iniciado.")
    print(f"[Admin] Escriba {NARANJA}'stop <cp_id>'{RESET} o {VERDE}'resume <cp_id>'{RESET} y pulse Enter.")
    
    while not stop_evt.is_set():
        try:
            # Esta línea se bloqueará, esperando la entrada del usuario
            command_line = input() 
            
            if stop_evt.is_set():
                break
                
            parts = command_line.strip().lower().split()
            if not parts:
                continue
                
            cmd = parts[0]
            if len(parts) < 2:
                print(f"{ROJO}[Admin] Error: Se requiere un comando y un cp_id (ej: stop CP001){RESET}")
                continue
                
            cp_id = parts[1].upper() # Estandarizar a mayúsculas
            
            if cmd == 'stop':
                # Enviar comando de Parada
                print(f"[Admin] Procesando comando STOP para {cp_id}...")
                process_admin_command(cp_id, "STOP", "Parado")
            elif cmd == 'resume':
                # Enviar comando de Reanudación
                print(f"[Admin] Procesando comando RESUME para {cp_id}...")
                process_admin_command(cp_id, "RESUME", "Activado")
            else:
                print(f"{ROJO}[Admin] Comando '{cmd}' no reconocido. Use 'stop' o 'resume'.{RESET}")
                
        except EOFError:
            break # Salir si se pulsa Ctrl+D
        except Exception as e:
            if not stop_evt.is_set():
                print(f"{ROJO}[Admin] Error en hilo de input: {e}{RESET}")
            
    print("[Admin] Hilo de input detenido.")

# Función de ayuda para procesar los comandos de admin
def process_admin_command(cp_id: str, kafka_action: str, db_state: str):
    """
    Actualiza el estado del CP en la BD local y envía el comando
    por Kafka al Engine correspondiente.
    """
    global kafka_producer
    
    with db_lock:
        if cp_id not in charging_points:
            print(f"{ROJO}[Admin] CP '{cp_id}' no encontrado en la base de datos.{RESET}")
            return
        
        current_state = charging_points[cp_id].get("state", "DESCONECTADO")
        if current_state == "DESCONECTADO":
            print(f"[Admin] IGNORADO: '{cp_id}' está DESCONECTADO. No se envía comando.")
            return {"ok": False, "error": f"CP '{cp_id}' no está conectado"}
            
        # Actualizar el estado local inmediatamente, el panel lo mostrará en el siguiente refresco
        charging_points[cp_id]['state'] = db_state
        print(f"[Admin] Estado local de '{cp_id}' actualizado a: {db_state}")

    # Enviar el comando por Kafka al CP (Engine)
    command_message = {
        'cpId': cp_id, 
        'action': kafka_action # "STOP" o "RESUME"
    }
    send_kafka_message('commands', command_message)
    print(f"[Admin] Comando '{kafka_action}' enviado a Kafka para {cp_id}.")


# Comunicación sockets
async def handle_client(reader: asyncio.StreamReader, writer: asyncio.StreamWriter):
    addr = writer.get_extra_info('peername')
    print(f"\n[Nueva Conexión] Recibida conexión de: {addr}")

    cp_id_conectado = None
    client_ip = addr[0] if addr else "unknown"

    try:
        while not stop_event_async.is_set():
            # intentar leer una línea, si no hay newline, hacer read
            try:
                linea = await asyncio.wait_for(reader.readline(), timeout=1.0)
            except asyncio.TimeoutError:
                # timeout para permitir comprobar stop_event_async periódicamente
                continue

            if not linea:
                print(f"[Desconexión] Cliente {addr} se ha desconectado.")
                break

            # Decodificar y procesar
            message = linea.decode('utf-8').rstrip('\n').rstrip('\r')
            if not message:
                # si viene vacío, continuar
                continue

            parts = message.split('#')
            display_message = message
            if parts[0] == "REGISTER" and len(parts) >= 3:
                display_message = f"{parts[0]}#{parts[1]}#<token>"
            print(f"[RX] {addr}: {display_message}")
            response = "ACK"

            if parts[0] == "REGISTER" and len(parts) >= 3:
                cp_id = parts[1]
                token = parts[2]
                cp_id_conectado = cp_id
                
                with db_lock:
                    if cp_id in charging_points:
                        ok, reason = validate_jwt(token, cp_id)
                        if ok:
                            charging_points[cp_id]['state'] = "Activado"
                            print(f"[Registro] CP '{cp_id}' (desde BD) se ha activado.")
                            aes_key = Fernet.generate_key()
                            with keys_lock:
                                cp_aes_keys[cp_id] = aes_key
                            response = f"ACK#KEY#{aes_key.decode('utf-8')}"
                            log_audit("cp_connected", client_ip, f"cp_id={cp_id}")
                        else:
                            response = f"NACK: TOKEN_INVALID ({reason})"
                            log_audit("critical_error", client_ip, f"registro_fallido cp_id={cp_id}")
                    else:
                        # Si no está en la BD, lo rechazamos
                        print(f"[Registro] RECHAZADO: CP '{cp_id}' no se encontró en la base de datos.")
                        response = "NACK: CP DESCONOCIDO"
            elif parts[0] == "REGISTER" and len(parts) == 2:
                response = "NACK: TOKEN_REQUERIDO"

            elif parts[0] == "FAULT" and len(parts) >= 2:
                cp_id = parts[1]
                cp_id_conectado = cp_id
                
                with db_lock:
                    if cp_id in charging_points:
                        if charging_points[cp_id]['state'] != "Averiado":
                            charging_points[cp_id]['state'] = "Averiado"
                            print(f"[Avería] CP '{cp_id}' ha reportado una avería.")
                            log_audit("critical_error", client_ip, f"cp_fault cp_id={cp_id}")
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

            elif message.upper() == "FIN" or parts[0].upper() == "FIN":
                response = "ADIOS"
                writer.write((response + "\n").encode('utf-8'))
                await writer.drain()
                print("[FIN] Cliente pidió terminar conexión")
                break

            else:
                print(f"[Mensaje] Mensaje no reconocido de {addr}: {message}")
                response = "NACK: Mensaje no reconocido"

            # Enviar respuesta
            writer.write((response + "\n").encode('utf-8'))
            await writer.drain()

    except (asyncio.IncompleteReadError, ConnectionResetError):
        print(f"[Info] Conexión con {addr} perdida o cerrada.")
        log_audit("critical_error", client_ip, "socket_disconnected_unexpected")
    except Exception as e:
        print(f"[Error] Excepción en handle_client: {e}")
        log_audit("critical_error", client_ip, f"socket_exception {e}")

    finally:
        if cp_id_conectado:
            with db_lock:
                if cp_id_conectado in charging_points:
                    # Obtenemos el estado actual
                    current_state = charging_points[cp_id_conectado]['state']

                    # Solo cambiamos a DESCONECTADO si el estado NO es "Suministrando" o "Parado".
                    # Si era "Activado" o "Averiado", debe pasar a "Desconectado".
                    if current_state != "Suministrando" and current_state != "Parado":
                        charging_points[cp_id_conectado]['state'] = "DESCONECTADO"
                        print(f"[Estado] CP '{cp_id_conectado}' (estado anterior: {current_state}) pasa a DESCONECTADO.")
                    else:
                        print(f"[Estado] CP '{cp_id_conectado}' desconectado, pero se mantiene estado {current_state}.")
            with keys_lock:
                cp_aes_keys.pop(cp_id_conectado, None)
            log_audit("cp_disconnected", client_ip, f"cp_id={cp_id_conectado}")
        try:
            writer.close()
            await writer.wait_closed()
        except:
            pass
        print(f"[Conexión Cerrada] Hilo para {addr} finalizado.")


# Funciones Kafka
def send_kafka_message(topic, message_data):
    """
    Función de ayuda para enviar mensajes con el productor global
    """
    global kafka_producer
    try:
        payload = json.dumps(message_data).encode('utf-8')
        kafka_producer.produce(topic, value=payload)
        kafka_producer.poll(0)  # no bloquear
    except Exception as e:
        print(f"[Error Kafka Produce] No se pudo enviar a {topic}: {e}")

def kafka_requests_consumer(broker, stop_evt: threading.Event):
    """ 
    Hilo que escucha permanentemente el topic "requests" 
    """
    print("[Kafka] Hilo consumidor de 'requests' iniciado.")

    while not stop_evt.is_set():
        consumer = None
        try:
            consumer_config = {
                'bootstrap.servers': broker,
                'group.id': f'central_requests_group',
                'auto.offset.reset': 'latest'
            }
            consumer = Consumer(consumer_config)
            consumer.subscribe(['requests'])
            print("[Kafka] Consumidor de 'requests' suscrito.")

            while not stop_evt.is_set():
                msg = consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    err_code = msg.error().code()
                    if err_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        print("[Kafka] Topic 'requests' no encontrado, re-intentando subscripción...")
                        break
                    else:
                        print(f"[Error Kafka Requests] {msg.error()}")
                        continue

                try:
                    request = json.loads(msg.value().decode('utf-8'))
                except Exception:
                    print("[Error Kafka Requests] Mensaje con JSON inválido.")
                    continue

                cp_id = request.get('cpId')
                driver_id = request.get('driverId')
                print(f"[Kafka Request] Recibida petición de {driver_id} para {cp_id}")

                with db_lock:
                    cp = charging_points.get(cp_id)

                if cp:
                    if cp['state'] == 'Activado':
                        # CP Libre: Autorizar
                        cp['state'] = 'Ocupado_temporal' # Marcamos temporalmente para evitar doble asignación rápida
                        auth_message = {'cpId': cp_id, 'driverId': driver_id, 'action': 'AUTHORIZE'}
                        send_kafka_message('commands', auth_message)
                        print(f"[Central] Autorizando inmediatamente a {driver_id} en {cp_id}")
                    elif cp['state'] == 'Suministrando':
                        # CP Ocupado: A la cola
                        print(f"[Central] CP {cp_id} ocupado. Añadiendo {driver_id} a la cola.")
                        pending_requests.append(request)
                    else:
                        # Averiado o Desconectado: Rechazar
                        reason = "CP Averiado/Desconectado"
                        ticket_message = {'driverId': driver_id, 'cpId': cp_id, 'status': 'REJECTED', 'reason': reason}
                        send_kafka_message('tickets', ticket_message)

        except Exception as e:
            print(f"[Error] Excepción grave en consumidor de 'requests': {e}")
        finally:
            if consumer:
                consumer.close()
            if not stop_evt.is_set():
                print("[Kafka] Consumidor 'requests' cerrado, reintentando en 3s...")
                time.sleep(3)

    print("[Kafka] Hilo consumidor 'requests' detenido.")

def kafka_telemetry_consumer(broker, stop_evt: threading.Event):
    """ 
    Hilo que escucha permanentemente el topic "telemetry" 
    """
    print("[Kafka] Hilo consumidor de 'telemetry' iniciado.")

    while not stop_evt.is_set():
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

            while not stop_evt.is_set():
                msg = consumer.poll(1.0)
                if msg is None: continue
                if msg.error(): continue # Gestión de errores simplificada para el ejemplo

                try:
                    raw_payload = json.loads(msg.value().decode('utf-8'))
                except Exception:
                    continue

                telemetry = raw_payload
                if isinstance(raw_payload, dict) and "ciphertext" in raw_payload:
                    cp_id = raw_payload.get("cpId")
                    with keys_lock:
                        key = cp_aes_keys.get(cp_id)
                    if not key:
                        log_audit("critical_error", "unknown", f"telemetry_key_missing cp_id={cp_id}")
                        continue
                    f = Fernet(key)
                    try:
                        decrypted = f.decrypt(raw_payload["ciphertext"].encode("utf-8"))
                        telemetry = json.loads(decrypted.decode("utf-8"))
                    except (InvalidToken, json.JSONDecodeError) as e:
                        log_audit("critical_error", "unknown", f"telemetry_decrypt_failed cp_id={cp_id} err={e}")
                        continue

                cp_id = telemetry.get('cpId')
                status = telemetry.get('status')

                with db_lock:
                    if cp_id not in charging_points: continue

                    # --- CORRECCIÓN AQUÍ ---
                    current_db_state = charging_points[cp_id]['state']

                    if status == 'CHARGING':
                        # PROTECCIÓN CONTRA RACE CONDITION:
                        # Si por socket ya nos han dicho que está AVERIADO, ignoramos este mensaje
                        # de "Cargando" que puede venir con retraso.
                        if current_db_state == "Averiado":
                            continue

                        charging_points[cp_id]['state'] = "Suministrando"
                        charging_points[cp_id]['driver'] = telemetry.get('driverId')
                        charging_points[cp_id]['consumo'] = telemetry.get('consumo_kw', 0.0)
                        charging_points[cp_id]['importe'] = telemetry.get('importe_eur', 0.0)
                    
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
                        
                        req_to_process = None
                        for req in pending_requests:
                            if req['cpId'] == cp_id:
                                req_to_process = req
                                break
                        
                        if req_to_process:
                            pending_requests.remove(req_to_process)
                            next_driver = req_to_process['driverId']
                            print(f"[Central] Cola: Asignando CP {cp_id} liberado a {next_driver}")
                            auth_message = {'cpId': cp_id, 'driverId': next_driver, 'action': 'AUTHORIZE'}
                            send_kafka_message('commands', auth_message)

                    elif status == 'STOPPED':
                        print(f"[Kafka Telemetry] Recibida parada (STOPPED) de {cp_id}")
                        charging_points[cp_id]['state'] = "Activado"
                        charging_points[cp_id]['driver'] = None
                        charging_points[cp_id]['consumo'] = 0.0
                        charging_points[cp_id]['importe'] = 0.0

                    # --- NUEVA GESTIÓN DE ERROR ---
                    elif status == 'ERROR':
                        print(f"[Kafka Telemetry] Recibido ERROR técnico de {cp_id}")
                        # Forzamos el estado a Averiado y limpiamos el driver
                        charging_points[cp_id]['state'] = "Averiado"
                        charging_points[cp_id]['driver'] = None
                        charging_points[cp_id]['consumo'] = 0.0
                        charging_points[cp_id]['importe'] = 0.0
                        log_audit("critical_error", "unknown", f"telemetry_error cp_id={cp_id}")

        except Exception as e:
            print(f"[Error] Excepción en telemetry: {e}")
        finally:
            if consumer: consumer.close()
            time.sleep(3)


def start_web_panel(http_host: str, http_port: int, kafka_broker: str):
    from panel_central import create_app

    def handle_web_command(cp_id, action):
        if action.startswith("CITY:"):
            new_city = action.split(":", 1)[1]
            print(f"[WEB CMD] Cambiando ciudad de {cp_id} a {new_city}")
            with db_lock:
                if cp_id in charging_points:
                    charging_points[cp_id]['city'] = new_city
                    # Reseteamos temperatura visualmente hasta que llegue el nuevo dato
                    charging_points[cp_id]['temp'] = None 
            save_database() 
        else:
            process_admin_command(cp_id, action, "Parado" if action == "STOP" else "Activado")

    # NUEVA FUNCIÓN PARA ACTUALIZAR TEMPERATURA
    def handle_weather_update(cp_id, temp):
        with db_lock:
            if cp_id in charging_points:
                charging_points[cp_id]['temp'] = temp

    app = create_app(
        state_getter=get_state_snapshot,
        command_sender=handle_web_command,
        weather_updater=handle_weather_update # <-- Pasamos el callback aquí
    )

    # Montar estáticos (Central/web)
    from fastapi.staticfiles import StaticFiles
    web_dir = os.path.join(os.path.dirname(__file__), "web")
    if os.path.isdir(web_dir):
        app.mount("/static", StaticFiles(directory=web_dir), name="static")

    config = uvicorn.Config(app=app, host=http_host, port=http_port, log_level="info")
    server = uvicorn.Server(config)

    def _run():
        server.run()

    t = Thread(target=_run, daemon=True)
    t.start()
    print(f"[WEB] Panel disponible en http://{http_host}:{http_port}")


async def main_async(listen_port: int, kafka_broker: str):
    global kafka_producer, stop_event_async

    stop_event_async = asyncio.Event()

    # Cargar DB
    load_database()

    # Inicializar Kafka producer
    try:
        kafka_producer = Producer({'bootstrap.servers': kafka_broker})
        print(f"[Kafka] Productor conectado a {kafka_broker}")
    except Exception as e:
        print(f"[Error Kafka] No se pudo conectar el Productor: {e}")
        return

    # Lanzar panel
    panel_task = asyncio.create_task(display_panel())

    # Lanzar panel web (hilo separado con uvicorn)
    try:
        start_web_panel(http_host="0.0.0.0", http_port=8000, kafka_broker=kafka_broker)
    except Exception as e:
        print(f"[WEB] Error al iniciar el panel web: {e}")

    # Lanzar consumidores Kafka en hilos
    req_thread = threading.Thread(target=kafka_requests_consumer, args=(kafka_broker, stop_event_threads), daemon=True)
    tel_thread = threading.Thread(target=kafka_telemetry_consumer, args=(kafka_broker, stop_event_threads), daemon=True)
    admin_thread = threading.Thread(target=admin_input_thread, args=(stop_event_threads,), daemon=True)
    req_thread.start()
    tel_thread.start()
    admin_thread.start()

    # Iniciar servidor TCP asíncrono
    server = await asyncio.start_server(handle_client, '0.0.0.0', listen_port, reuse_address=True, backlog=16)
    addrs = ', '.join(str(sock.getsockname()) for sock in server.sockets)
    print(f"\n*** EV_Central iniciada ***")
    print(f"Escuchando Sockets en: {addrs}")
    print("El panel de control se está mostrando. Esperando conexiones...")

    # Registrar manejadores de señal
    loop = asyncio.get_running_loop()

    def _signal_handler():
        print("[SINAL] Señal de parada recibida. Iniciando apagado limpio...")
        stop_event_threads.set()   # para hilos
        # set asyncio event desde hilo/handler
        loop.call_soon_threadsafe(stop_event_async.set)

    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            signal.signal(sig, lambda *_: _signal_handler())

    async with server:
        await stop_event_async.wait()
        print("[MAIN] stop_event_async activado, cerrando servidor...")
        server.close()
        await server.wait_closed()

    # Esperar tareas y cerrar hilos Kafka
    print("[MAIN] Esperando a que los hilos Kafka terminen...")
    stop_event_threads.set()
    req_thread.join(timeout=5)
    tel_thread.join(timeout=5)

    # Cancelar panel si sigue vivo
    if not panel_task.done():
        panel_task.cancel()
        try:
            await panel_task
        except asyncio.CancelledError:
            pass

    # Vaciar productor Kafka
    try:
        kafka_producer.flush(timeout=10)
    except Exception as e:
        print(f"[Kafka] Error al flush producer: {e}")

    print("[CENTRAL] Apagado limpio completado.")

def main():
    if len(sys.argv) < 3:
        print("Error: Debes especificar un puerto y el broker de Kafka.")
        print("Uso: python EV_Central.py <puerto_socket> <kafka_broker_host:port>")
        return

    try:
        listen_port = int(sys.argv[1])
        kafka_broker = sys.argv[2]
    except ValueError:
        print("Error: El puerto debe ser un número entero.")
        return

    try:
        asyncio.run(main_async(listen_port, kafka_broker))
    except KeyboardInterrupt:
        print("\n[MAIN] Interrupción por teclado.")

if __name__ == "__main__":
    main()

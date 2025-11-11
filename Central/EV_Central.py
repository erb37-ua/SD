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


# Colores
VERDE = '\033[92m'
NARANJA = '\033[38;5;208m'
ROJO = '\033[91m'
GRIS = '\033[90m'
RESET = '\033[0m'

# Estado CPs
charging_points = {}

# Kafka consumers se ejecutan en hilos
db_lock = threading.Lock()

# Kafka producer global
kafka_producer = None

# Eventos para parada limpia
stop_event_threads = threading.Event()   # para hilos
stop_event_async = None                  # será asyncio.Event()

def get_state_snapshot():
    """
    Devuelve una copia segura del estado de los CPs para el panel web.
    """
    with db_lock:
        # copia superficial de dicts (suficiente para lectura)
        return {cp_id: dict(data) for cp_id, data in charging_points.items()}

# Funciones de BD / panel
def load_database(filename="cp_database.txt"):
    """
    Carga los CPs conocidos desde un archivo de texto al iniciar
    Formato esperado por línea: cp_id,location,price
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
                        try:
                            price = float(parts[2])
                        except:
                            price = 0.50
                        with db_lock:
                            charging_points[cp_id] = {
                                "location": location,
                                "price": price,
                                "state": "DESCONECTADO",
                                "driver": None,
                                "consumo": 0.0,
                                "importe": 0.0
                            }
        print(f"[Info] Cargados {len(charging_points)} CPs.")
    except FileNotFoundError:
        print(f"[Error] No se encontró el archivo {filename}. Empezando con 0 CPs.")
    except Exception as e:
        print(f"[Error] Al cargar {filename}: {e}")

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

            print(f"[RX] {addr}: {message}")
            parts = message.split('#')
            response = "ACK"

            if parts[0] == "REGISTER" and len(parts) >= 2:
                cp_id = parts[1]
                cp_id_conectado = cp_id
                
                with db_lock:
                    if cp_id in charging_points:
                        charging_points[cp_id]['state'] = "Activado"
                        print(f"[Registro] CP '{cp_id}' (desde BD) se ha activado.")
                        response = "ACK: Registrado y Activado"
                    else:
                        # Si no está en la BD, lo rechazamos
                        print(f"[Registro] RECHAZADO: CP '{cp_id}' no se encontró en la base de datos.")
                        response = "NACK: CP DESCONOCIDO"

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
    except Exception as e:
        print(f"[Error] Excepción en handle_client: {e}")

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

                if cp and cp['state'] == 'Activado':
                    auth_message = {'cpId': cp_id, 'driverId': driver_id, 'action': 'AUTHORIZE'}
                    send_kafka_message('commands', auth_message)
                    print(f"[Kafka Command] Autorización enviada a {cp_id} para {driver_id}")
                else:
                    reason = "CP no disponible o averiado" if cp else "CP desconocido"
                    ticket_message = {'driverId': driver_id, 'cpId': cp_id, 'status': 'REJECTED', 'reason': reason}
                    send_kafka_message('tickets', ticket_message)
                    print(f"[Kafka Ticket] Petición rechazada para {driver_id} ({reason})")

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
                if msg is None:
                    continue
                if msg.error():
                    err_code = msg.error().code()
                    if err_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                        print("[Kafka] Topic 'telemetry' no encontrado, re-intentando subscripción...")
                        break
                    else:
                        print(f"[Error Kafka Telemetry] {msg.error()}")
                        continue

                try:
                    telemetry = json.loads(msg.value().decode('utf-8'))
                except Exception:
                    print("[Error Kafka Telemetry] Mensaje con JSON inválido.")
                    continue

                cp_id = telemetry.get('cpId')
                status = telemetry.get('status')

                with db_lock:
                    if cp_id not in charging_points:
                        continue

                    if status == 'CHARGING':
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
                    
                    elif status == 'STOPPED':
                        print(f"[Kafka Telemetry] Recibida parada (STOPPED) de {cp_id}")
                        # NO enviar ticket, solo resetear el estado del CP
                        charging_points[cp_id]['state'] = "Activado"
                        charging_points[cp_id]['driver'] = None
                        charging_points[cp_id]['consumo'] = 0.0
                        charging_points[cp_id]['importe'] = 0.0

        except Exception as e:
            print(f"[Error] Excepción grave en consumidor de 'telemetry': {e}")
        finally:
            if consumer:
                consumer.close()
            if not stop_evt.is_set():
                print("[Kafka] Consumidor 'telemetry' cerrado, reintentando en 3s...")
                time.sleep(3)

    print("[Kafka] Hilo consumidor 'telemetry' detenido.")

def start_web_panel(http_host: str, http_port: int, kafka_broker: str):
    """
    Lanza el servidor FastAPI/Uvicorn en un hilo.
    Comparte el estado y la función de comandos con el panel.
    """
    from panel_central import create_app  # import diferido para evitar ciclos

    app = create_app(
        state_getter=get_state_snapshot,
        command_sender=lambda cp_id, action: process_admin_command(
            cp_id, action, "Parado" if action == "STOP" else "Activado"
        ),
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

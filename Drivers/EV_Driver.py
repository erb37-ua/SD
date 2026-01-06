import sys
import json
import time
import threading
from confluent_kafka import Producer, Consumer

# Eventos de control
session_active_event = threading.Event() # Controla la conexión con el CP (Heartbeats)
charge_finished_event = threading.Event() # Controla el ciclo de una carga específica

def delivery_report(err, msg):
    if err is not None:
        print(f"[Kafka] Fallo de entrega: {err}")

# --- HILO: Heartbeat de Sesión (Constante) ---
def heartbeat_sender(producer, driver_id, cp_id):
    """
    Envía latidos constantemente mientras el usuario esté en el menú del CP.
    Independiente de si está cargando o no.
    """
    print(f"[{driver_id}] Iniciando sesión (Heartbeat) con {cp_id}...")
    while session_active_event.is_set():
        try:
            hb_data = {
                'driverId': driver_id,
                'cpId': cp_id,
                'action': 'HEARTBEAT'
            }
            # Enviar y forzar salida inmediata (flush)
            producer.produce('commands', value=json.dumps(hb_data).encode('utf-8'))
            producer.flush() 
            time.sleep(2) # Latido cada 2 segundos
        except Exception as e:
            print(f"[Heartbeat] Error: {e}")
            break
    print(f"[{driver_id}] Fin de sesión (Heartbeat detenido).")

# --- HILO: Escucha de Kafka ---
def kafka_listener(broker, driver_id, cp_id):
    """ Escucha Tickets y Telemetría """
    consumer = None
    try:
        consumer = Consumer({
            'bootstrap.servers': broker,
            'group.id': f'driver_{driver_id}_session',
            'auto.offset.reset': 'latest'
        })
        consumer.subscribe(['tickets', 'telemetry'])
        
        while session_active_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None or msg.error(): continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                if data.get('cpId') != cp_id: continue

                # Detectar Avería del CP (Telemetría)
                if msg.topic() == 'telemetry':
                    if data.get('status') == 'ERROR':
                        print(f"\n[ALERTA] El CP reporta AVERÍA: {data.get('reason')}")
                        # Si estábamos cargando, esto libera el bloqueo de carga
                        charge_finished_event.set()
                        # Opcional: Si la avería es crítica, podríamos cerrar la sesión también:
                        # session_active_event.clear()

                # Detectar Fin de Carga (Ticket)
                elif msg.topic() == 'tickets' and data.get('driverId') == driver_id:
                    print(f"\n--- TICKET RECIBIDO ---")
                    print(f"  Estado: {data.get('status')}")
                    print(f"  Total: {data.get('final_consumo_kw')} kWh | {data.get('final_importe_eur')} €")
                    print("-----------------------")
                    charge_finished_event.set()

            except Exception:
                pass
    finally:
        if consumer: consumer.close()

def send_command(producer, driver_id, cp_id, action):
    data = {'driverId': driver_id, 'cpId': cp_id, 'action': action}
    producer.produce('commands', value=json.dumps(data).encode('utf-8'))
    producer.flush()

def main():
    if len(sys.argv) < 3:
        print("Uso: python EV_Driver.py <broker> <driver_id>")
        return

    broker = sys.argv[1]
    driver_id = sys.argv[2]
    producer = Producer({'bootstrap.servers': broker})

    try:
        while True:
            print(f"\n--- DRIVER {driver_id} ---")
            print("Escribe el ID del CP (ej: CP001) para conectar, o 'exit' para salir.")
            cp_input = input(">> ").strip()
            
            if cp_input.lower() == 'exit': break
            if not cp_input: continue
            
            # 1. INICIAR SESIÓN (CONEXIÓN)
            current_cp = cp_input
            session_active_event.set() # Activar sesión
            
            # Arrancar hilos de fondo (Heartbeat y Escucha)
            t_hb = threading.Thread(target=heartbeat_sender, args=(producer, driver_id, current_cp))
            t_li = threading.Thread(target=kafka_listener, args=(broker, driver_id, current_cp))
            t_hb.start()
            t_li.start()

            # 2. SUB-MENÚ DE ACCIONES
            while session_active_event.is_set():
                print(f"\n--- Conectado a {current_cp} ---")
                print("1. Solicitar Carga")
                print("2. Desconectar (Salir)")
                opt = input("Opción: ").strip()

                if opt == '1':
                    print("Enviando solicitud de carga...")
                    charge_finished_event.clear() # Resetear evento de carga
                    
                    # Enviar Request
                    req = {'driverId': driver_id, 'cpId': current_cp}
                    producer.produce('requests', value=json.dumps(req).encode('utf-8'))
                    producer.flush()
                    
                    print("Esperando carga... (Pulsa Ctrl+C para parada de emergencia)")
                    try:
                        # Esperar a que termine la carga (por ticket o error)
                        # Simulamos un bucle de espera que permite KeyboardInterrupt
                        while not charge_finished_event.is_set():
                            time.sleep(0.5)
                        print("Proceso de carga finalizado.")
                    except KeyboardInterrupt:
                        print("\n[!] Enviando Parada de Emergencia...")
                        send_command(producer, driver_id, current_cp, 'STOP')
                        time.sleep(1) # Dar tiempo a procesar
                    
                elif opt == '2':
                    print("Cerrando conexión...")
                    session_active_event.clear() # Esto matará los hilos de hb y listener
                    break
            
            # Esperar a que los hilos mueran limpiamente antes de volver al menú principal
            t_hb.join()
            t_li.join()

    except KeyboardInterrupt:
        print("\nApagando Driver...")
    finally:
        session_active_event.clear()
        producer.flush()

if __name__ == "__main__":
    main()
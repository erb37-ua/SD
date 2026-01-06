import sys
import json
import time
import threading
from confluent_kafka import Producer, Consumer, KafkaError

# Evento global para coordinar los hilos
charge_complete_event = threading.Event()

def delivery_report(err, msg):
    """ Callback de productor """
    if err is not None:
        print(f"Fallo al enviar mensaje: {err}")

# --- NUEVO: Hilo de Envío de Heartbeat ---
def heartbeat_sender(producer, driver_id, cp_id):
    """
    Envía un pulso al CP cada 2 segundos para indicar que el Driver sigue vivo.
    Si este script muere, el CP dejará de recibir esto y cortará la carga.
    """
    print(f"[{driver_id}] Iniciando envío de Heartbeat a {cp_id}...")
    while not charge_complete_event.is_set():
        try:
            hb_data = {
                'driverId': driver_id,
                'cpId': cp_id,
                'action': 'HEARTBEAT'
            }
            producer.produce('commands', value=json.dumps(hb_data).encode('utf-8'))
            producer.poll(0)
            time.sleep(2) # Enviar cada 2 segundos
        except Exception as e:
            print(f"[Heartbeat] Error enviando latido: {e}")
            break
    print(f"[{driver_id}] Heartbeat detenido.")

def kafka_listener(broker: str, driver_id: str, cp_id: str):
    """
    Escucha 'tickets' (para fin normal) y 'telemetry' (para detectar averías del CP).
    """
    consumer = None
    try:
        consumer_config = {
            'bootstrap.servers': broker,
            'group.id': f'driver_group_{driver_id}',
            'auto.offset.reset': 'latest'
        }
        consumer = Consumer(consumer_config)
        # MODIFICADO: Escuchar también telemetry para saber si el CP se avería
        consumer.subscribe(['tickets', 'telemetry'])
        print(f"[{driver_id}] Escuchando Tickets y Telemetría de {cp_id}...")

        while not charge_complete_event.is_set():
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue
            
            if msg.error():
                continue
            
            try:
                topic = msg.topic()
                data = json.loads(msg.value().decode('utf-8'))
                
                # Filtrar mensajes que no sean de nuestro CP o Driver
                if data.get('cpId') != cp_id:
                    continue

                # CASO 1: Recibimos un TICKET (Fin de carga normal)
                if topic == 'tickets' and data.get('driverId') == driver_id:
                    print(f"\n--- TICKET RECIBIDO ---")
                    print(f"  Estado: {data.get('status')}")
                    if data.get('status') == 'COMPLETED':
                        print(f"  Consumo Final: {data.get('final_consumo_kw')} kWh")
                        print(f"  Importe Final: {data.get('final_importe_eur')} €")
                    else:
                        print(f"  Razón: {data.get('reason')}")
                    print("-----------------------")
                    charge_complete_event.set()

                # CASO 2: Recibimos TELEMETRÍA (Monitorizar averías del CP)
                elif topic == 'telemetry':
                    # Si el CP reporta estado ERROR o el driverId coincide y status es Fault
                    status = data.get('status')
                    if status == 'ERROR':
                        print(f"\n[ALERTA] El CP {cp_id} ha reportado una AVERÍA/ERROR.")
                        print(f"Razón: {data.get('reason', 'Unknown')}")
                        print("Deteniendo interfaz del conductor...")
                        charge_complete_event.set()

            except json.JSONDecodeError:
                pass
            except Exception as e:
                print(f"[Error Listener] {e}")

    except Exception as e:
        print(f"[Error Listener] Excepción grave: {e}")
    finally:
        if consumer:
            consumer.close()

def send_stop_command(producer: Producer, driver_id: str, cp_id: str):
    print(f"[!] Enviando petición de PARADA manual...")
    try:
        command_data = {'driverId': driver_id, 'cpId': cp_id, 'action': 'STOP'}
        producer.produce('commands', value=json.dumps(command_data).encode('utf-8'), callback=delivery_report)
        producer.flush()
    except Exception as e:
        print(f"[Error] No se pudo enviar parada: {e}")

def main():
    if len(sys.argv) < 3:
        print("Uso: python EV_Driver.py <broker_host:port> <driver_id>")
        return

    broker = sys.argv[1]
    driver_id = sys.argv[2]

    producer_config = {'bootstrap.servers': broker}
    producer = Producer(producer_config)
    
    try:
        while True:
            print("\n--- MENÚ PRINCIPAL (Driver) ---")
            print("Introduzca el ID del CP para INICIAR CARGA (ej: CP001) o 'q' para salir:")
            cp_id_input = input("Opción: ").strip()

            if cp_id_input.lower() == 'q':
                break
            if not cp_id_input:
                continue

            current_cp_id = cp_id_input
            charge_complete_event.clear()

            # 1. Hilo Escucha (Tickets y Errores de CP)
            listener_thread = threading.Thread(
                target=kafka_listener,
                args=(broker, driver_id, current_cp_id),
                daemon=True
            )
            listener_thread.start()

            # 2. Hilo Heartbeat (Mantener viva la conexión)
            hb_thread = threading.Thread(
                target=heartbeat_sender,
                args=(producer, driver_id, current_cp_id),
                daemon=True
            )
            
            # Enviar Petición de Inicio
            try:
                request_data = {'driverId': driver_id, 'cpId': current_cp_id}
                producer.produce('requests', value=json.dumps(request_data).encode('utf-8'), callback=delivery_report)
                producer.poll(0)
                print(f"\n[{driver_id}] Petición enviada. Esperando inicio de carga...")
            except Exception as e:
                print(f"[Error] Fallo al enviar petición: {e}")
                charge_complete_event.set()
                continue

            # Arrancamos el heartbeat (asumimos que empezará a cargar pronto)
            hb_thread.start()

            print(f"[{driver_id}] Interfaz activa. Pulsa 'p' y Enter para PARAR MANUALMENTE.")
            
            # Bucle de entrada de usuario (Bloqueante)
            while not charge_complete_event.is_set():
                try:
                    # Usamos select o input con timeout simulado es complejo en python puro multiplataforma
                    # para simplificar, usamos input bloqueante en un hilo o polling simple si no hay input.
                    # Aquí mantenemos el input bloqueante, pero si el evento se activa (por ticket o error CP),
                    # necesitamos que el usuario pulse enter o gestionarlo.
                    # Para simplificar la UX en consola:
                    if sys.platform == 'win32':
                         import msvcrt
                         if msvcrt.kbhit():
                             key = msvcrt.getch()
                             if key.lower() == b'p':
                                 send_stop_command(producer, driver_id, current_cp_id)
                                 break
                         time.sleep(0.5)
                    else:
                        # Versión simplificada para Linux/Mac (requiere Enter)
                        # Nota: Si el CP falla, el mensaje saldrá, pero el input seguirá esperando un Enter.
                        # Es una limitación de consola simple.
                        import select
                        i, o, e = select.select( [sys.stdin], [], [], 1 )
                        if (i):
                            user_input = sys.stdin.readline().strip()
                            if user_input.lower() == 'p':
                                send_stop_command(producer, driver_id, current_cp_id)
                                break
                        
                except KeyboardInterrupt:
                    send_stop_command(producer, driver_id, current_cp_id)
                    time.sleep(1)
                    sys.exit(0)

            print(f"[{driver_id}] Sesión finalizada. Deteniendo hilos...")
            charge_complete_event.set() # Asegurar parada
            hb_thread.join(timeout=1)
            listener_thread.join(timeout=1)
            time.sleep(1)

    except KeyboardInterrupt:
        print("\nSaliendo...")
    finally:
        producer.flush()

if __name__ == "__main__":
    main()
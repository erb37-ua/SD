import sys
import json
import time
from confluent_kafka import Producer, Consumer, KafkaError
import uuid
import threading

# Evento global para coordinar los hilos, se usará para avisar que la recarga ha terminado (por ticket/parada)
charge_complete_event = threading.Event()

def delivery_report(err, msg):
    """ 
    Callback de productor: se llama cuando un mensaje es entregado o falla. 
    """
    if err is not None:
        print(f"Fallo al enviar mensaje: {err}")


def kafka_ticket_listener(broker: str, driver_id: str, cp_id: str):
    """
    Se ejecuta en un hilo separado.
    Crea su propio consumidor y espera el ticket de finalización.
    """
    consumer = None
    try:
        consumer_config = {
            'bootstrap.servers': broker,
            'group.id': f'driver_group_{driver_id}',
            'auto.offset.reset': 'latest'
        }
        consumer = Consumer(consumer_config)
        consumer.subscribe(['tickets'])
        print(f"[{driver_id}] Hilo listener de 'tickets' iniciado, buscando {cp_id}...")

        while not charge_complete_event.is_set(): # Sigue escuchando hasta que el evento se active
            msg = consumer.poll(timeout=1.0)
            if msg is None: 
                continue
            
            if msg.error():
                print(f"[Error Hilo Kafka] {msg.error()}")
                continue
            
            try:
                ticket = json.loads(msg.value().decode('utf-8'))
                
                # Comprobar si es NUESTRO ticket
                if ticket.get('driverId') == driver_id and ticket.get('cpId') == cp_id:
                    print(f"\n--- TICKET RECIBIDO (para {cp_id}) ---")
                    print(f"  Estado: {ticket.get('status')}")
                    if ticket.get('status') == 'COMPLETED':
                        print(f"  Consumo: {ticket.get('final_consumo_kw')} kWh")
                        print(f"  Importe: {ticket.get('final_importe_eur')} €")
                    else:
                        print(f"  Razón: {ticket.get('reason')}")
                    print("-------------------------")
                    
                    charge_complete_event.set() # Avisa al hilo principal que se ha termindado

            except json.JSONDecodeError:
                print("[Error Hilo Kafka] Error al decodificar ticket JSON.")
            except Exception as e:
                print(f"[Error Hilo Kafka] {e}")

    except Exception as e:
        print(f"[Error Hilo Kafka] Excepción grave: {e}")
    finally:
        if consumer:
            consumer.close()
        print(f"[{driver_id}] Hilo listener de 'tickets' detenido.")


def send_stop_command(producer: Producer, driver_id: str, cp_id: str):
    """
    Envía una petición de parada al topic 'commands'.
    """
    print(f"[!] Enviando petición de PARADA para {cp_id}...")
    try:
        command_data = {
            'driverId': driver_id,
            'cpId': cp_id,
            'action': 'STOP' # El engine debe ser modificado para entender esto
        }
        payload = json.dumps(command_data).encode('utf-8')
        
        producer.produce('commands', value=payload, callback=delivery_report)
        producer.flush() # Asegurar que se envía antes de continuar
        print(f"[!] Petición de PARADA enviada.")
    except Exception as e:
        print(f"[Error Productor] No se pudo enviar la parada: {e}")


def main():
    if len(sys.argv) < 3:
        print("Uso: python EV_Driver.py <broker_host:port> <driver_id>")
        return

    broker = sys.argv[1]
    driver_id = sys.argv[2]

    producer_config = {'bootstrap.servers': broker}
    producer = Producer(producer_config)
    
    # El consumidor se creará dentro del hilo de escucha

    try:
        # Bucle interactivo principal
        while True:
            print("\n--- MENÚ PRINCIPAL (Driver) ---")
            print("Introduzca el ID del CP para INICIAR CARGA (ej: CP001):")
            print("Escriba 'q' para salir.")
            
            cp_id_input = input("Opción: ").strip()

            if cp_id_input.lower() == 'q':
                print(f"[{driver_id}] Saliendo...")
                break
            
            if not cp_id_input:
                continue

            current_cp_id = cp_id_input
            
            # Limpiar el evento por si se usó antes
            charge_complete_event.clear()

            # Crear y arrancar el Hilo de Escucha Kafka
            listener_thread = threading.Thread(
                target=kafka_ticket_listener,
                args=(broker, driver_id, current_cp_id),
                daemon=True # El hilo morirá si el programa principal cierra
            )
            listener_thread.start()

            # Enviar la petición de recarga (en el hilo principal)
            try:
                request_data = {
                    'driverId': driver_id,
                    'cpId': current_cp_id
                }
                payload = json.dumps(request_data).encode('utf-8')
                producer.produce('requests', value=payload, callback=delivery_report)
                producer.poll(0)
                print(f"\n[{driver_id}] Petición enviada para CP: {current_cp_id}")
            except Exception as e:
                print(f"[Error Productor] No se pudo enviar la petición: {e}")
                charge_complete_event.set() # Avisa al hilo listener que pare
                continue # Volver al menú

            # El hilo principal se queda esperando la entrada del usuario
            print(f"[{driver_id}] Recarga en curso... (Pulsa 'p' y Enter para PARAR)")
            
            while not charge_complete_event.is_set():
                try:
                    # Este input() bloquea el hilo principal, mientras el otro hilo escucha Kafka
                    user_input = input() 
                    if user_input.lower() == 'p':
                        print("... Petición de PARADA recibida.")
                        send_stop_command(producer, driver_id, current_cp_id)
                        charge_complete_event.set() # Avisa al hilo listener que pare
                        break # Salir del bucle de input
                except KeyboardInterrupt:
                    print(f"\n[{driver_id}] ¡Ctrl+C detectado! Enviando parada de emergencia...")
                    send_stop_command(producer, driver_id, current_cp_id)
                    charge_complete_event.set()
                    # Esperamos un momento para asegurar que Kafka envíe el mensaje antes de morir
                    time.sleep(1) 
                    sys.exit(0) # Salimos del programa
                except EOFError:
                    break # Salir si se presiona Ctrl+D
            
            # La recarga ha terminado (por ticket o por 'p')
            print(f"[{driver_id}] Sesión de recarga para {current_cp_id} finalizada.")
            listener_thread.join(timeout=2.0) # Espera a que el hilo listener termine
            print("Volviendo al menú principal...")
            time.sleep(1)

    except KeyboardInterrupt:
        print(f"\n[{driver_id}] Proceso interrumpido.")
        charge_complete_event.set() # Asegurarse de que todos los hilos mueran
    finally:
        print(f"[{driver_id}] Cerrando productor.")
        producer.flush()

if __name__ == "__main__":
    main()
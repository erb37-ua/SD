import sys
import json
import time
import threading
from confluent_kafka import Producer, Consumer

# EVENTOS DE CONTROL
session_active_event = threading.Event()  # Controla el envío de Heartbeats (Sesión activa)
charge_finished_event = threading.Event() # Controla el bucle de espera durante la carga

def delivery_report(err, msg):
    """Callback para confirmar envío de mensajes"""
    if err is not None:
        print(f"[Kafka] Error envío: {err}")

# HILO 1: Heartbeat (Latido constante)
def heartbeat_sender(producer, driver_id, cp_id):
    """
    Envía un latido cada 2 segundos SIEMPRE que estemos conectados al menú del CP.
    Esto le dice al CP: "Sigo aquí, no me he cerrado".
    """
    print(f"[{driver_id}] Iniciando Heartbeat con {cp_id}...")
    while session_active_event.is_set():
        try:
            hb_data = {
                'driverId': driver_id,
                'cpId': cp_id,
                'action': 'HEARTBEAT'
            }
            
            producer.produce('commands', value=json.dumps(hb_data).encode('utf-8'))
            producer.flush() 
            time.sleep(2) 
        except Exception as e:
            print(f"[Heartbeat] Error: {e}")
            break
    print(f"[{driver_id}] Heartbeat detenido.")

# HILO 2: Escucha (Respuestas del CP)
def kafka_listener(broker, driver_id, cp_id):
    """
    Escucha Tickets (fin de carga) y Telemetría (para detectar averías).
    """
    consumer = None
    try:
        # Usamos un group.id único con timestamp para evitar bloqueossi reiniciamos la terminal muy rápido.
        consumer = Consumer({
            'bootstrap.servers': broker,
            'group.id': f'driver_{driver_id}_{int(time.time())}',
            'auto.offset.reset': 'latest'
        })
        consumer.subscribe(['tickets', 'telemetry'])
        
        while session_active_event.is_set():
            msg = consumer.poll(1.0)
            if msg is None or msg.error(): continue

            try:
                data = json.loads(msg.value().decode('utf-8'))
                
                # Ignorar mensajes de otros CPs
                if data.get('cpId') != cp_id: continue

                topic = msg.topic()

                # CASO A: El CP reporta una AVERÍA o ERROR
                if topic == 'telemetry':
                    if data.get('status') == 'ERROR':
                        print(f"\n\n[ALERTA] El CP reporta ERROR: {data.get('reason')}")
                        print("Deteniendo carga...")
                        # Desbloqueamos el hilo principal si estaba esperando
                        charge_finished_event.set()

                # CASO B: Recibimos un TICKET (Fin de carga normal)
                elif topic == 'tickets' and data.get('driverId') == driver_id:
                    print(f"\n\n--- TICKET FINAL ---")
                    print(f"  Estado: {data.get('status')}")
                    print(f"  Energía: {data.get('final_consumo_kw')} kWh")
                    print(f"  Coste:   {data.get('final_importe_eur')} €")
                    print("--------------------")
                    charge_finished_event.set()

            except Exception:
                pass
    except Exception as e:
        print(f"[Listener] Error grave: {e}")
    finally:
        if consumer: consumer.close()

def send_command(producer, driver_id, cp_id, action):
    """Ayuda para enviar comandos simples"""
    data = {'driverId': driver_id, 'cpId': cp_id, 'action': action}
    producer.produce('commands', value=json.dumps(data).encode('utf-8'))
    producer.flush()

def main():
    if len(sys.argv) < 3:
        print("Uso: python EV_Driver.py <broker> <driver_id>")
        return

    broker = sys.argv[1]
    driver_id = sys.argv[2]
    
    # Productor para enviar comandos
    producer = Producer({'bootstrap.servers': broker})

    try:
        while True:
            print(f"\n=== DRIVER {driver_id} ===")
            print("Introduce ID del CP (ej: CP001) o 'exit':")
            cp_input = input(">> ").strip()
            
            if cp_input.lower() == 'exit': break
            if not cp_input: continue
            
            current_cp = cp_input
            
            # 1. ACTIVAR SESIÓN
            # Esto arranca los latidos. Si cierras la terminal ahora, 
            # el latido parará y el CP se enterará.
            session_active_event.set() 
            
            t_hb = threading.Thread(target=heartbeat_sender, args=(producer, driver_id, current_cp))
            t_li = threading.Thread(target=kafka_listener, args=(broker, driver_id, current_cp))
            
            t_hb.start()
            t_li.start()

            # 2. MENÚ DE CONEXIÓN
            try:
                while session_active_event.is_set():
                    print(f"\n--- Conectado a {current_cp} ---")
                    print("1. Iniciar Carga")
                    print("2. Desconectar (Volver al inicio)")
                
                    opt = input("Opción: ").strip()

                    if opt == '1':
                        print(">> Solicitando carga al CP...")
                        charge_finished_event.clear() 
                    
                        req = {'driverId': driver_id, 'cpId': current_cp}
                        producer.produce('requests', value=json.dumps(req).encode('utf-8'))
                        producer.flush()
                        
                        print(">> Esperando carga... (Ctrl+C para Emergencia)")
                        
                        # Bucle de espera no bloqueante para poder recibir señales de stop
                        try:
                            while not charge_finished_event.is_set():
                                time.sleep(0.5)
                            print(">> Ciclo de carga terminado.")
                        except KeyboardInterrupt:
                            print("\n[!] ENVIANDO PARADA DE EMERGENCIA...")
                            send_command(producer, driver_id, current_cp, 'STOP')
                            # Esperamos un poco para recibir el ticket
                            time.sleep(2)
                        
                    elif opt == '2':
                        print("Desconectando...")
                        session_active_event.clear() # Rompe el bucle y para los hilos
                        break
            except KeyboardInterrupt:
                print("\nInterrupción detectada, saliendo al menú principal...")
                session_active_event.clear()

            # Asegurar limpieza de hilos antes de volver a empezar
            t_hb.join(timeout=1)
            t_li.join(timeout=1)

    except KeyboardInterrupt:
        print("\nCerrando Driver...")
    finally:
        session_active_event.clear()
        producer.flush()

if __name__ == "__main__":
    main()
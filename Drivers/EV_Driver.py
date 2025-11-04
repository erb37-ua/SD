import sys
import json
import time
from confluent_kafka import Producer, Consumer, KafkaError
import uuid

def delivery_report(err, msg):
    """ Callback de productor: se llama cuando un mensaje es entregado o falla. """
    if err is not None:
        print(f"Fallo al enviar mensaje: {err}")
    else:
        print(f"Mensaje enviado a {msg.topic()} [{msg.partition()}]")

def main():
    if len(sys.argv) < 4:
        print("Uso: python EV_Driver.py <broker> <driver_id> <fichero_requests>")
        return

    broker = sys.argv[1]
    driver_id = sys.argv[2]
    requests_file = sys.argv[3]

    print(f"[{driver_id}] Iniciando... Dando 5s para que los CPs se registren...")
    time.sleep(5)

    # Configuración del Productor
    producer_config = {'bootstrap.servers': broker}
    producer = Producer(producer_config)
    
    # Configuración del Consumidor (para tickets)
    consumer_config = {
        'bootstrap.servers': broker,
        'group.id': f'driver_group_{driver_id}_{uuid.uuid4()}', # Group ID único
        'auto.offset.reset': 'earliest' # Solo queremos tickets nuevos
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe(['tickets']) # Topic para recibir tickets

    try:
        with open(requests_file, 'r') as f:
            cp_requests = f.read().splitlines()

        print(f"Driver '{driver_id}' iniciado. Procesando {len(cp_requests)} peticiones de '{requests_file}'...")

        for cp_id_raw in cp_requests:
            # Esta variable 'cp_id' ahora es local al bucle for
            cp_id = cp_id_raw.strip()
            if not cp_id:
                continue

            # 1. Enviar la petición de recarga
            request_data = {
                'driverId': driver_id,
                'cpId': cp_id
            }
            payload = json.dumps(request_data).encode('utf-8')
            
            producer.produce('requests', value=payload, callback=delivery_report)
            producer.poll(0)
            print(f"\n[{driver_id}] Petición enviada para CP: {cp_id}")

            # 2. Esperar el ticket de conclusión
            print(f"[{driver_id}] Esperando finalización de recarga para {cp_id}...")
            charge_completed = False
            
            while not charge_completed:
                consumer = None
                try:
                    consumer_config = {
                        'bootstrap.servers': broker,
                        'group.id': f'driver_group_{driver_id}_{uuid.uuid4()}',
                        'auto.offset.reset': 'earliest' 
                    }
                    consumer = Consumer(consumer_config)
                    consumer.subscribe(['tickets'])
                    print(f"[{driver_id}] Consumidor de 'tickets' suscrito (buscando {cp_id}).")

                    while not charge_completed: 
                        msg = consumer.poll(timeout=1.0)
                        if msg is None: continue
                        
                        if msg.error():
                            err_code = msg.error().code()
                            if err_code == KafkaError.UNKNOWN_TOPIC_OR_PART:
                                print(f"[{driver_id}] Topic 'tickets' no encontrado, re-intentando subscripción...")
                                break 
                            else:
                                print(f"[Error Consumidor Driver] {msg.error()}")
                                continue
                        
                        try:
                            ticket = json.loads(msg.value().decode('utf-8'))
                            
                            # CAMBIO 2: Comprobar que el ticket es para esta petición
                            if ticket.get('driverId') == driver_id and ticket.get('cpId') == cp_id:
                                print(f"--- TICKET RECIBIDO (para {cp_id}) ---")
                                print(f"  Estado: {ticket.get('status')}")
                                if ticket.get('status') == 'COMPLETED':
                                    print(f"  Consumo: {ticket.get('final_consumo_kw')} kWh")
                                    print(f"  Importe: {ticket.get('final_importe_eur')} €")
                                else:
                                    print(f"  Razón: {ticket.get('reason')}")
                                print("-------------------------")
                                
                                charge_completed = True
                            
                            # Ignorar tickets de otras peticiones
                            elif ticket.get('driverId') == driver_id:
                                print(f"[{driver_id}] Ignorando ticket antiguo para {ticket.get('cpId')}...")

                        except json.JSONDecodeError:
                            print("Error: No se pudo decodificar el ticket (JSON).")

                except Exception as e:
                    print(f"[Error] Excepción grave en consumidor de 'tickets': {e}")
                finally:
                    if consumer:
                        consumer.close()
                    if not charge_completed:
                        print(f"[{driver_id}] Consumidor 'tickets' cerrado, re-intentando en 3s...")
                        time.sleep(3)
            
            # 3. Esperar 4 segundos antes de la siguiente petición
            print(f"[{driver_id}] Esperando 4 segundos para la siguiente petición...")
            time.sleep(4)

        print(f"[{driver_id}] Todas las peticiones han sido procesadas.")

    except FileNotFoundError:
        print(f"Error: No se encuentra el fichero '{requests_file}'")
    except KeyboardInterrupt:
        print(f"[{driver_id}] Proceso interrumpido.")
    finally:
        producer.flush()
        consumer.close()

if __name__ == "__main__":
    main()
import socket
import sys
import time

def connect_to_engine(engine_ip, engine_port):
    """
    Función de ayuda para reconectar al Engine
    """
    try:
        # Crear un nuevo socket para el Engine
        engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        engine_socket.connect((engine_ip, engine_port))
        print(f"[Monitor] Conectado al Engine en {engine_ip}:{engine_port}")
        return engine_socket
    except socket.error:
        # No imprimimos error aquí, la lógica principal ya lo hace
        return None

def main():
    # 1. Validar argumentos de línea de comandos
    if len(sys.argv) < 6:
        print("Error: Faltan argumentos.")
        print("Uso: python EV_CP_M.py <IP_Central> <Puerto_Central> <ID_CP> <IP_Engine> <Puerto_Engine>")
        return

    central_ip = sys.argv[1]
    cp_id = sys.argv[3]
    engine_ip = sys.argv[4]

    try:
        central_port = int(sys.argv[2])
        engine_port = int(sys.argv[5])
    except ValueError:
        print("Error: Los puertos deben ser números enteros.")
        return

    # Socket 1: Conexión con EV_Central
    try:
        central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print(f"[{cp_id}] Conectando a EV_Central en {central_ip}:{central_port}...")
        central_socket.connect((central_ip, central_port))
        print(f"[{cp_id}] ¡Conectado a CENTRAL!")

        # 4. Enviar mensaje de registro
        location = "C/Ficticia 123"
        message = f"REGISTER#{cp_id}#{location}"
        print(f"[{cp_id}] Enviando registro: {message}")
        central_socket.sendall(message.encode('utf-8'))

        # 5. Esperar respuesta (ACK) del servidor
        response_data = central_socket.recv(1024)
        print(f"[{cp_id}] Respuesta de CENTRAL: {response_data.decode('utf-8')}")

        # Socket 2: Conexión con EV_CP_E (Engine)
        engine_socket = connect_to_engine(engine_ip, engine_port)
        
        last_reported_status = "" # Para evitar enviar mensajes redundantes
        
        while True:
            health_status = "KO" # Asumimos KO por defecto

            if engine_socket:
                try:
                    engine_socket.sendall("HEALTH_CHECK".encode('utf-8'))
                    engine_response = engine_socket.recv(1024).decode('utf-8')
                    if engine_response == "OK":
                        health_status = "OK"
                
                except socket.error:
                    print(f"[{cp_id}] Error de comunicación con Engine. Reintentando conexión...")
                    engine_socket.close()
                    engine_socket = connect_to_engine(engine_ip, engine_port)
                    health_status = "KO"
            else:
                print(f"[{cp_id}] Desconectado del Engine. Intentando reconectar...")
                engine_socket = connect_to_engine(engine_ip, engine_port)
                health_status = "KO"

            # 3. Reportar estado a la CENTRAL (solo si cambia)
            try:
                if health_status != last_reported_status:
                    if health_status == "KO":
                        central_socket.sendall(f"FAULT#{cp_id}".encode('utf-8'))
                    else:
                        central_socket.sendall(f"HEALTHY#{cp_id}".encode('utf-8'))
                    
                    central_socket.recv(1024) # Esperar ACK
                    last_reported_status = health_status

            except socket.error:
                print(f"[{cp_id}] ¡Error de conexión con CENTRAL! Saliendo...")
                break

            time.sleep(1)

    except socket.error as e:
        print(f"[{cp_id}] Error de Socket (CENTRAL): {e}")
    except KeyboardInterrupt:
        print(f"\n[{cp_id}] Desconectando...")
    finally:
        # 6. Cerrar ambos sockets
        print(f"[{cp_id}] Cerrando conexiones.")
        if 'central_socket' in locals():
            central_socket.close()
        if 'engine_socket' in locals() and engine_socket:
            engine_socket.close()

if __name__ == "__main__":
    main()
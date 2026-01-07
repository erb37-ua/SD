import socket
import sys
import time
import os
import requests

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

def send_key_to_engine(engine_socket, aes_key):
    if not engine_socket:
        return False
    try:
        engine_socket.sendall(f"SET_KEY#{aes_key}\n".encode("utf-8"))
        engine_socket.settimeout(3)
        response = engine_socket.recv(1024).decode("utf-8").strip()
        engine_socket.settimeout(None)
        return response == "KEY_OK"
    except socket.error:
        return False

def write_token_log(cp_id, token):
    log_path = os.getenv("TOKEN_LOG_PATH")
    if not log_path:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        log_path = os.path.join(base_dir, "registry_tokens.log")
    try:
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} | cp_id={cp_id} | token={token}\n")
        return log_path
    except OSError as exc:
        print(f"[{cp_id}] No se pudo escribir el token en {log_path}: {exc}")
        return None

def write_central_key_log(cp_id, aes_key):
    log_path = os.getenv("CENTRAL_KEY_LOG_PATH")
    if not log_path:
        base_dir = os.path.dirname(os.path.abspath(__file__))
        log_path = os.path.join(base_dir, "central_keys.log")
    try:
        timestamp = time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
        with open(log_path, "a", encoding="utf-8") as f:
            f.write(f"{timestamp} | cp_id={cp_id} | key={aes_key}\n")
        return log_path
    except OSError as exc:
        print(f"[{cp_id}] No se pudo escribir la clave en {log_path}: {exc}")
        return None

def get_registry_token(registry_url, cp_id, location, verify_ssl):
    url = registry_url.rstrip("/") + "/register"
    try:
        resp = requests.post(
            url,
            json={"cp_id": cp_id, "location": location},
            timeout=5,
            verify=verify_ssl,
        )
        resp.raise_for_status()
        data = resp.json()
        token = data.get("token")
        if not token:
            print(f"[{cp_id}] Registro sin token válido.")
            return None
        return token
    except Exception as e:
        print(f"[{cp_id}] Error registrando en EV_Registry: {e}")
        return None

def main():
    # Validar argumentos de línea de comandos
    if len(sys.argv) >= 6:
        central_ip = sys.argv[1]
        cp_id = sys.argv[3]
        engine_ip = sys.argv[4]
        try:
            central_port = int(sys.argv[2])
            engine_port = int(sys.argv[5])
        except ValueError:
            print("Error: Los puertos deben ser números enteros.")
            return
    else:
        central_ip = os.getenv("CENTRAL_HOST")
        cp_id = os.getenv("CP_ID")
        engine_ip = os.getenv("ENGINE_HOST")
        central_port = os.getenv("CENTRAL_PORT")
        engine_port = os.getenv("ENGINE_PORT")
        if not all([central_ip, cp_id, engine_ip, central_port, engine_port]):
            print("Error: Faltan argumentos o variables de entorno.")
            print("Uso: python EV_CP_M.py <IP_Central> <Puerto_Central> <ID_CP> <IP_Engine> <Puerto_Engine>")
            return
        central_port = int(central_port)
        engine_port = int(engine_port)

    registry_url = os.getenv("REGISTRY_URL", "https://registry:8080")
    cp_location = os.getenv("CP_LOCATION", "unknown")
    verify_ssl = os.getenv("REGISTRY_VERIFY_SSL", "false").lower() in ("1", "true", "yes")
    cert_path = os.getenv("REGISTRY_CERT_PATH")
    verify_setting = cert_path if cert_path else verify_ssl

    token = get_registry_token(registry_url, cp_id, cp_location, verify_setting)
    if not token:
        return
    token_log_path = write_token_log(cp_id, token)
    if token_log_path:
        print(f"[{cp_id}] Token guardado en {token_log_path}")

    # Socket 1: Conexión con EV_Central
    try:
        central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print(f"[{cp_id}] Conectando a EV_Central en {central_ip}:{central_port}...")
        central_socket.connect((central_ip, central_port))
        print(f"[{cp_id}] ¡Conectado a CENTRAL!")

        # Enviar mensaje de registro
        message = f"REGISTER#{cp_id}#{token}\n"
        print(f"[{cp_id}] Enviando registro a CENTRAL (token oculto).")
        central_socket.sendall(message.encode('utf-8'))

        # Esperar respuesta (ACK) del servidor
        response_data = central_socket.recv(1024).decode("utf-8").strip()
        display_response = response_data
        if response_data.startswith("ACK#KEY#"):
            display_response = "ACK#KEY#<clave>"
        print(f"[{cp_id}] Respuesta de CENTRAL: {display_response}")
        if response_data.startswith("ACK#KEY#"):
            aes_key = response_data.split("#", 2)[2]
            key_log_path = write_central_key_log(cp_id, aes_key)
            if key_log_path:
                print(f"[{cp_id}] Clave guardada en {key_log_path}")
        else:
            print(f"[{cp_id}] Registro rechazado o sin clave AES.")
            return

        # Socket 2: Conexión con EV_CP_E (Engine)
        engine_socket = connect_to_engine(engine_ip, engine_port)
        if engine_socket:
            if not send_key_to_engine(engine_socket, aes_key):
                print(f"[{cp_id}] No se pudo enviar la clave AES al Engine.")
        
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
                    if engine_socket:
                        send_key_to_engine(engine_socket, aes_key)
                    health_status = "KO"
            else:
                print(f"[{cp_id}] Desconectado del Engine. Intentando reconectar...")
                engine_socket = connect_to_engine(engine_ip, engine_port)
                if engine_socket:
                    send_key_to_engine(engine_socket, aes_key)
                health_status = "KO"

            # Reportar estado a la CENTRAL (solo si cambia)
            try:
                if health_status != last_reported_status:
                    if health_status == "KO":
                        message = f"FAULT#{cp_id}\n"
                    else:
                        message = f"HEALTHY#{cp_id}\n"
                    
                    central_socket.sendall(message.encode('utf-8'))
                    
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
        # Cerrar ambos sockets
        print(f"[{cp_id}] Cerrando conexiones.")
        if 'central_socket' in locals():
            central_socket.close()
        if 'engine_socket' in locals() and engine_socket:
            engine_socket.close()

if __name__ == "__main__":
    main()

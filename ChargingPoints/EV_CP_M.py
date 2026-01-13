import socket
import sys
import time
import os
import requests
from urllib.parse import urlparse

def connect_to_engine(engine_ip, engine_port):
    """
    Función de ayuda para reconectar al Engine
    """
    try:
        engine_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        engine_socket.connect((engine_ip, engine_port))
        print(f"[Monitor] Conectado al Engine en {engine_ip}:{engine_port}")
        return engine_socket
    except socket.error:
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
<<<<<<< HEAD
    registry_ip_arg = None

=======
>>>>>>> d0395ac964f9b77ee28ce7718eecedcbd33e2b1b
    def normalize_cp_id(value):
        return (value or "").strip().upper()

    def validate_port(raw_value, label):
        try:
            port = int(raw_value)
        except (TypeError, ValueError):
            raise ValueError(f"{label} debe ser un entero.")
        if port <= 0 or port > 65535:
            raise ValueError(f"{label} fuera de rango.")
        return port

    def validate_host(value, label):
        host = (value or "").strip()
        if not host:
            raise ValueError(f"{label} requerido.")
        return host

    def validate_cp_id(raw_value):
        cp_id = normalize_cp_id(raw_value)
        if not cp_id or not cp_id.isalnum():
            raise ValueError("CP_ID inválido.")
        return cp_id

    def validate_registry_url(value):
        url = (value or "").strip()
        if not url:
            raise ValueError("REGISTRY_URL requerido.")
        parsed = urlparse(url)
        if parsed.scheme not in ("http", "https") or not parsed.netloc:
            raise ValueError("REGISTRY_URL inválida.")
        return url

    if len(sys.argv) >= 6:
        central_ip = sys.argv[1]
        cp_id = sys.argv[3]
        engine_ip = sys.argv[4]
        try:
            central_port = validate_port(sys.argv[2], "CENTRAL_PORT")
            engine_port = validate_port(sys.argv[5], "ENGINE_PORT")
        except ValueError as exc:
            print(f"Error: {exc}")
            return
        
        if len(sys.argv) >= 7:
            registry_ip_arg = sys.argv[6]

    else:
        central_ip = os.getenv("CENTRAL_HOST")
        cp_id = os.getenv("CP_ID")
        engine_ip = os.getenv("ENGINE_HOST")
        central_port = os.getenv("CENTRAL_PORT")
        engine_port = os.getenv("ENGINE_PORT")
        
        registry_ip_arg = os.getenv("REGISTRY_HOST")

        if not all([central_ip, cp_id, engine_ip, central_port, engine_port]):
            print("Error: Faltan argumentos o variables de entorno.")
            print("Uso: python EV_CP_M.py <IP_Central> <Puerto_Central> <ID_CP> <IP_Engine> <Puerto_Engine> [IP_Registry]")
            return
        try:
            central_port = validate_port(central_port, "CENTRAL_PORT")
            engine_port = validate_port(engine_port, "ENGINE_PORT")
        except ValueError as exc:
            print(f"Error: {exc}")
            return
<<<<<<< HEAD
=======
        try:
            central_port = validate_port(central_port, "CENTRAL_PORT")
            engine_port = validate_port(engine_port, "ENGINE_PORT")
        except ValueError as exc:
            print(f"Error: {exc}")
            return

    try:
        central_ip = validate_host(central_ip, "CENTRAL_HOST")
        engine_ip = validate_host(engine_ip, "ENGINE_HOST")
        cp_id = validate_cp_id(cp_id)
    except ValueError as exc:
        print(f"Error: {exc}")
        return
>>>>>>> d0395ac964f9b77ee28ce7718eecedcbd33e2b1b

    try:
        central_ip = validate_host(central_ip, "CENTRAL_HOST")
        engine_ip = validate_host(engine_ip, "ENGINE_HOST")
        cp_id = validate_cp_id(cp_id)
    except ValueError as exc:
        print(f"Error: {exc}")
        return

    # Configuracion Registry URL
    registry_url = os.getenv("REGISTRY_URL", "https://registry:8080")

    if registry_ip_arg:
        # Asumimos puerto 8080 y https como en el código de referencia
        registry_url = f"https://{registry_ip_arg}:8080"
        print(f"[{cp_id}] Configurado Registry manual en: {registry_url}")

    cp_location = os.getenv("CP_LOCATION", "unknown")
    verify_ssl = os.getenv("REGISTRY_VERIFY_SSL", "false").lower() in ("1", "true", "yes")
    cert_path = os.getenv("REGISTRY_CERT_PATH")
    verify_setting = cert_path if cert_path else verify_ssl
<<<<<<< HEAD
    
=======
>>>>>>> d0395ac964f9b77ee28ce7718eecedcbd33e2b1b
    try:
        registry_url = validate_registry_url(registry_url)
    except ValueError as exc:
        print(f"Error: {exc}")
        return

    cp_location = (cp_location or "").strip()
    if not cp_location:
        print("Error: CP_LOCATION requerido.")
        return

    token = get_registry_token(registry_url, cp_id, cp_location, verify_setting)
    if not token:
        return
    token_log_path = write_token_log(cp_id, token)
    if token_log_path:
        print(f"[{cp_id}] Token guardado en {token_log_path}")

    try:
        central_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print(f"[{cp_id}] Conectando a EV_Central en {central_ip}:{central_port}...")
        central_socket.connect((central_ip, central_port))
        print(f"[{cp_id}] ¡Conectado a CENTRAL!")

        message = f"REGISTER#{cp_id}#{token}\n"
        print(f"[{cp_id}] Enviando registro a CENTRAL (token oculto).")
        central_socket.sendall(message.encode('utf-8'))

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

        engine_socket = connect_to_engine(engine_ip, engine_port)
        if engine_socket:
            if not send_key_to_engine(engine_socket, aes_key):
                print(f"[{cp_id}] No se pudo enviar la clave AES al Engine.")
        
        last_reported_status = "" 
        
        while True:
            health_status = "KO"

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

            try:
                if health_status != last_reported_status:
                    if health_status == "KO":
                        message = f"FAULT#{cp_id}\n"
                    else:
                        message = f"HEALTHY#{cp_id}\n"
                    
                    central_socket.sendall(message.encode('utf-8'))
                    
                    central_socket.recv(1024)
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
        print(f"[{cp_id}] Cerrando conexiones.")
        if 'central_socket' in locals():
            central_socket.close()
        if 'engine_socket' in locals() and engine_socket:
            engine_socket.close()

if __name__ == "__main__":
    main()
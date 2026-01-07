# Registry/EV_Registry.py
import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import jwt
import secrets
import sys
import os
import time

# Definimos el modelo de datos que esperamos recibir del CP
class CPRegisterRequest(BaseModel):
    cp_id: str
    location: str

JWT_SECRET = os.getenv("REGISTRY_JWT_SECRET", "dev_registry_secret")
JWT_ALG = "HS256"

# Almacén en memoria de los CPs registrados
# Formato: { "CP001": {"location": "...", "token": "..."} }
registered_cps = {}

app = FastAPI(title="EV Registry Service")

@app.post("/register")
def register_cp(request: CPRegisterRequest):
    """
    Endpoint para registrar un CP.
    Recibe el ID y la ubicación.
    Devuelve un token de acceso único.
    """
    cp_id = request.cp_id
    
    issued_at = int(time.time())
    token_payload = {
        "cp_id": cp_id,
        "location": request.location,
        "iat": issued_at,
        "exp": issued_at + 24 * 3600,
        "nonce": secrets.token_hex(8),
    }
    token = jwt.encode(token_payload, JWT_SECRET, algorithm=JWT_ALG)
    if isinstance(token, bytes):
        token = token.decode("utf-8")
    
    # Guardamos el CP y su token "en la base de datos" (memoria)
    registered_cps[cp_id] = {
        "location": request.location,
        "token": token
    }
    
    print(f"[REGISTRY] Nuevo registro: {cp_id} en {request.location}. Token generado.")
    
    # Devolvemos el token al CP
    return {"status": "registered", "token": token}

@app.get("/check/{cp_id}")
def check_cp(cp_id: str):
    """
    Endpoint (opcional) para que la Central verifique si un CP es válido.
    """
    if cp_id in registered_cps:
        return {"valid": True, "data": registered_cps[cp_id]}
    else:
        raise HTTPException(status_code=404, detail="CP not found")

def main():
    # Argumentos por línea de comandos para el puerto (igual que tus otros scripts)
    if len(sys.argv) < 2:
        port = 8080 # Puerto por defecto si no se pasa argumento
    else:
        port = int(sys.argv[1])

    print(f"*** EV_Registry iniciando en puerto {port} (HTTPS) ***")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    cert_path = os.path.join(base_dir, "certServ.pem")

    if not os.path.exists(cert_path):
        print(f"[ERROR] No encuentro el certificado en: {cert_path}")
        # Listar archivos para ver qué pasa
        print(f"Archivos en {base_dir}: {os.listdir(base_dir)}")
        return

    uvicorn.run(
        app, 
        host="0.0.0.0", 
        port=port,
        ssl_certfile=cert_path
    )

if __name__ == "__main__":
    main()

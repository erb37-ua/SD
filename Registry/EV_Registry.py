import uvicorn
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import jwt
import secrets
import sys
import os
import time
import json

class CPRegisterRequest(BaseModel):
    cp_id: str
    location: str

JWT_SECRET = os.getenv("REGISTRY_JWT_SECRET", "dev_registry_secret")
JWT_ALG = "HS256"

DB_FILE = "/app/cp_database.json"

registered_cps = {}

app = FastAPI(title="EV Registry Service")

def update_token_in_db(cp_id, token):
    """Actualiza el token del CP en el JSON compartido."""
    if not os.path.exists(DB_FILE):
        print(f"[ERROR] No se encuentra la BD en {DB_FILE}")
        return False
        
    try:
        # 1. Leer archivo
        with open(DB_FILE, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        # 2. Buscar CP y actualizar token
        found = False
        for cp in data:
            if cp.get("id") == cp_id:
                cp["auth_token"] = token # Guardamos el token
                found = True
                break
        
        if not found:
            print(f"[WARN] El CP {cp_id} no existe en la BD. No se guardó el token.")
            return False

        # 3. Guardar cambios
        with open(DB_FILE, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2)
            
        print(f"[DB] Token guardado para {cp_id}")
        return True

    except Exception as e:
        print(f"[ERROR] Fallo al escribir en BD: {e}")
        return False

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
    
    update_token_in_db(cp_id, token)
    
    registered_cps[cp_id] = {
        "location": request.location,
        "token": token
    }
    
    print(f"[REGISTRY] Nuevo registro: {cp_id} en {request.location}. Token generado.")
    
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
    if len(sys.argv) < 2:
        port = 8080 
    else:
        port = int(sys.argv[1])

    print(f"*** EV_Registry iniciando en puerto {port} (HTTPS) ***")

    base_dir = os.path.dirname(os.path.abspath(__file__))
    cert_path = os.path.join(base_dir, "certServ.pem")

    if not os.path.exists(cert_path):
        print(f"[ERROR] No encuentro el certificado en: {cert_path}")
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

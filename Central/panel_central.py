# Central/panel_central.py
from typing import Callable, Dict, Any, List, Optional
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException
from fastapi.responses import HTMLResponse
from pydantic import BaseModel

"""
Servidor Web para el panel de control.
- WebSockets para el Frontend.
- API REST para módulos externos (EV_W).
"""

# Modelos de datos para la API
class ExternalCommand(BaseModel):
    cp_id: str
    reason: str = "Weather Alert"

# Modelo para recibir temperatura
class WeatherUpdate(BaseModel):
    cp_id: str
    temperature: float

def create_app(
    state_getter: Callable[[], Dict[str, Dict[str, Any]]],
    command_sender: Callable[[str, str], None],
    weather_updater: Callable[[str, float], None] = None,
    apikey_handler: Callable[[str], None] = None
) -> FastAPI:
    app = FastAPI(title="EV Central Panel & API")

    # Lista de websockets conectados
    active_clients: List[WebSocket] = []

    # FRONTEND
    @app.get("/")
    async def root():
        return HTMLResponse(
            """
            <!doctype html>
            <html>
              <head>
                <meta charset="utf-8" />
                <title>EV Central Panel</title>
              </head>
              <body>
                <script>window.location.href='/static/index.html';</script>
              </body>
            </html>
            """
        )

    @app.get("/health")
    async def health():
        return {"status": "ok"}
    
    # API REST
    @app.post("/api/alert")
    async def receive_alert(cmd: ExternalCommand):
        """
        Recibe una alerta (ej: Clima malo).
        Equivale a un comando STOP administrativo.
        """
        print(f"[API] Recibida ALERTA para {cmd.cp_id}: {cmd.reason}")
        try:
            command_sender(cmd.cp_id, "STOP")
            return {"status": "processed", "action": "STOP", "cp_id": cmd.cp_id}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/resume")
    async def receive_resume(cmd: ExternalCommand):
        """
        Recibe una orden de reanudación (ej: Clima mejora).
        Equivale a un comando RESUME administrativo.
        """
        print(f"[API] Recibida REANUDACIÓN para {cmd.cp_id}: {cmd.reason}")
        try:
            command_sender(cmd.cp_id, "RESUME")
            return {"status": "processed", "action": "RESUME", "cp_id": cmd.cp_id}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
    
    @app.post("/api/weather")
    async def receive_weather(data: WeatherUpdate):
        if weather_updater:
            weather_updater(data.cp_id, data.temperature)
        return {"status": "updated"}

    # WEBSOCKETS
    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        await ws.accept()
        active_clients.append(ws)
        try:
            await ws.send_json({"type": "snapshot", "data": state_getter()})
            
            async def receiver():
                while True:
                    msg = await ws.receive_json()
                    if isinstance(msg, dict) and msg.get("type") == "command":
                        cp_id = msg.get("cpId")
                        raw_action = msg.get("action") or ""

                        # LÓGICA DE SISTEMA (API KEY)
                        if cp_id == 'SYSTEM':
                            if raw_action.startswith('SET_APIKEY:') and apikey_handler:
                                key = raw_action.split(':', 1)[1]
                                apikey_handler(key) # Enviamos la clave a EV_Central -> EV_W
                                await ws.send_json({"type": "ack", "ok": True})
                            elif raw_action == 'REVOKE_APIKEY' and apikey_handler:
                                apikey_handler(None) # Revocamos
                                await ws.send_json({"type": "ack", "ok": True})
                            continue

                        if raw_action.startswith("CITY:"):
                            final_action = raw_action # Mantenemos mayúsculas/minúsculas de la ciudad
                        else:
                            final_action = raw_action.upper()
                        
                        if cp_id and (final_action in ("STOP", "RESUME") or final_action.startswith("CITY:")):
                            try:
                                command_sender(cp_id, final_action) # Enviamos final_action
                                await ws.send_json({"type": "ack", "ok": True})
                            except Exception as e:
                                await ws.send_json({"type": "ack", "ok": False, "error": str(e)})
            
            async def broadcaster():
                while True:
                    await asyncio.sleep(1)
                    try:
                        await ws.send_json({"type": "snapshot", "data": state_getter()})
                    except Exception:
                        break

            await asyncio.gather(asyncio.create_task(receiver()), asyncio.create_task(broadcaster()))

        except WebSocketDisconnect:
            pass
        finally:
            if ws in active_clients:
                active_clients.remove(ws)

    return app
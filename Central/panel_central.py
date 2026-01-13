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

class ExternalCommand(BaseModel):
    cp_id: str
    reason: str = "Weather Alert"

class WeatherUpdate(BaseModel):
    cp_id: str
    temperature: float

class WeatherKeyUpdate(BaseModel):
    api_key: str

def create_app(
    state_getter: Callable[[], Dict[str, Dict[str, Any]]],
    command_sender: Callable[[str, str], None],
    weather_updater: Callable[[str, float], None] = None,
    weather_key_setter: Callable[[str], None] = None
) -> FastAPI:
    app = FastAPI(title="EV Central Panel & API")

    active_clients: List[WebSocket] = []

    def normalize_cp_id(raw_id: str) -> str:
        return (raw_id or "").strip().upper()

    def ensure_valid_cp_id(cp_id: str) -> str:
        normalized = normalize_cp_id(cp_id)
        if not normalized or not normalized.isalnum():
            raise HTTPException(status_code=422, detail="cp_id inválido")
        return normalized

    # --- FRONTEND ---
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
    
    # --- API REST ---
    @app.post("/api/alert")
    async def receive_alert(cmd: ExternalCommand):
        """
        Recibe una alerta (ej: Clima malo).
        Equivale a un comando STOP administrativo.
        """
        cp_id = ensure_valid_cp_id(cmd.cp_id)
        print(f"[API] Recibida ALERTA para {cp_id}: {cmd.reason}")
        try:
            command_sender(cp_id, "STOP")
            return {"status": "processed", "action": "STOP", "cp_id": cp_id}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))

    @app.post("/api/resume")
    async def receive_resume(cmd: ExternalCommand):
        """
        Recibe una orden de reanudación (ej: Clima mejora).
        Equivale a un comando RESUME administrativo.
        """
        cp_id = ensure_valid_cp_id(cmd.cp_id)
        print(f"[API] Recibida REANUDACIÓN para {cp_id}: {cmd.reason}")
        try:
            command_sender(cp_id, "RESUME")
            return {"status": "processed", "action": "RESUME", "cp_id": cp_id}
        except Exception as e:
            raise HTTPException(status_code=400, detail=str(e))
    
    @app.post("/api/weather")
    async def receive_weather(data: WeatherUpdate):
        cp_id = ensure_valid_cp_id(data.cp_id)
        if not (-80.0 <= data.temperature <= 80.0):
            raise HTTPException(status_code=422, detail="temperature fuera de rango")
        if weather_updater:
            weather_updater(cp_id, data.temperature)
        return {"status": "updated"}

    @app.post("/api/config/weather-key")
    async def receive_weather_key(data: WeatherKeyUpdate):
        if not weather_key_setter:
            raise HTTPException(status_code=501, detail="weather_key_not_supported")
        api_key = (data.api_key or "").strip()
        if not api_key:
            raise HTTPException(status_code=422, detail="api_key requerido")
        if len(api_key) < 10:
            raise HTTPException(status_code=422, detail="api_key demasiado corto")
        weather_key_setter(api_key)
        return {"status": "saved"}

    # --- WEBSOCKETS ---
    @app.get("/management/status")
    async def management_status():
        state = state_getter()
        summary = {}
        for cp in state.values():
            st = cp.get("state", "UNKNOWN")
            summary[st] = summary.get(st, 0) + 1
        return {"total": len(state), "by_state": summary, "cp_ids": sorted(state.keys())}

    @app.post("/clima")
    async def clima(payload: Dict[str, Any]):
        if climate_handler is None:
            return {"ok": False, "error": "climate_handler_not_configured"}
        return climate_handler(payload)

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

                        if raw_action.startswith("CITY:"):
                            final_action = raw_action
                        else:
                            final_action = raw_action.upper()
                            
                        if cp_id and (final_action in ("STOP", "RESUME") or final_action.startswith("CITY:")):
                            try:
                                cp_id = ensure_valid_cp_id(cp_id)
                            except HTTPException as exc:
                                await ws.send_json({"type": "ack", "ok": False, "error": exc.detail})
                                continue
                            try:
                                command_sender(cp_id, final_action) 
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

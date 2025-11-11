# Central/panel_central.py
from typing import Callable, Dict, Any, List
import asyncio
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse

"""
Servidor Web para el panel de control.

- Expone WebSocket en /ws
- Envía 'snapshots' del estado de CPs de forma periódica y cuando hay clientes conectados
- Recibe comandos {type:"command", cpId:"CP001", action:"STOP"|"RESUME"} y
  los reenvía a la central mediante 'command_sender'
"""

def create_app(
    state_getter: Callable[[], Dict[str, Dict[str, Any]]],
    command_sender: Callable[[str, str], None],
) -> FastAPI:
    app = FastAPI(title="EV Central Panel")

    # Lista de websockets conectados
    active_clients: List[WebSocket] = []

    @app.get("/")
    async def root():
        # Servimos el index.html desde /static para simplificar
        return HTMLResponse(
            """
            <!doctype html>
            <html>
              <head>
                <meta charset="utf-8" />
                <meta name="viewport" content="width=device-width, initial-scale=1" />
                <title>EV Central Panel</title>
                <link rel="stylesheet" href="/static/index.html" />
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

    @app.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        await ws.accept()
        active_clients.append(ws)
        try:
            # Enviamos un snapshot inicial inmediatamente
            await ws.send_json({"type": "snapshot", "data": state_getter()})

            # Dos tareas concurrentes:
            #  1) Recibir mensajes (comandos)
            #  2) Emitir snapshots periódicos
            async def receiver():
                while True:
                    msg = await ws.receive_json()
                    # Esperamos formato: {"type":"command","cpId":"CP001","action":"STOP"}
                    if isinstance(msg, dict) and msg.get("type") == "command":
                        cp_id = msg.get("cpId")
                        action = (msg.get("action") or "").upper()
                        if cp_id and action in ("STOP", "RESUME"):
                            try:
                                command_sender(cp_id, action)
                                await ws.send_json({"type": "ack", "ok": True})
                            except Exception as e:
                                await ws.send_json({"type": "ack", "ok": False, "error": str(e)})
                        else:
                            await ws.send_json({"type": "ack", "ok": False, "error": "Bad command"})
                    else:
                        # ignorar mensajes no reconocidos
                        pass

            async def broadcaster():
                # envia snapshots cada 1s mientras el socket esté vivo
                while True:
                    await asyncio.sleep(1)
                    # si no hay clientes, esta tarea se cancela cuando se cae la conexión
                    try:
                        await ws.send_json({"type": "snapshot", "data": state_getter()})
                    except Exception:
                        break

            recv_task = asyncio.create_task(receiver())
            bcast_task = asyncio.create_task(broadcaster())
            await asyncio.gather(recv_task, bcast_task)

        except WebSocketDisconnect:
            pass
        finally:
            if ws in active_clients:
                active_clients.remove(ws)

    return app

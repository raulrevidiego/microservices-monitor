from __future__ import annotations

import asyncio
import logging
from typing import Optional

from fastapi import WebSocket, WebSocketDisconnect

from core.models import MetricSnapshot

logger = logging.getLogger(__name__)


class WebSocketManager:
    """Gestor de Conexiones WebSocket (Patrón Centralizado).
        Se encarga de llevar el registro de todos los clientes conectados (ej: paneles de Streamlit)
        y retransmitirles las métricas en tiempo real."""

    def __init__(self) -> None:
        self._connections: list[WebSocket] = [] #Lista en memoria de todas las conexiones WebSocket activas.
        self._lock = asyncio.Lock() #Candado asincrono para garantizar que solo una tarea modifique las lista.

    #Gestión de conexiones.

    async def connect(self, websocket: WebSocket) -> None:
        #Acepta una nueva petición de llamada WebSocket y la añade a la lista de conexiones activas.
        await websocket.accept()
        #Guarda la conexión en la lista usando el candado.
        async with self._lock:
            self._connections.append(websocket)
        logger.info(
            "WebSocket conectado — total: %d",
            len(self._connections),
        )

    async def disconnect(self, websocket: WebSocket) -> None:
        #Elimina una conexión WebSocket de la lista de conexiones activas.
        async with self._lock:
            try:
                self._connections.remove(websocket)
            except ValueError:
                pass
        logger.info(
            "WebSocket desconectado — total: %d",
            len(self._connections),
        )

    async def disconnect_all(self) -> None:
        #Cierra todas las conexiones WebSocket activas de forma ordenada y limpia la lista.
        async with self._lock:
            connections = list(self._connections)
            self._connections.clear()
        for ws in connections:
            try:
                await ws.close()
            except Exception:
                pass

    #Difusión y envío de datos.

    async def broadcast_snapshot(self, snapshot: MetricSnapshot) -> None:
        """Retransmite la foto de métricas actual a TODOS los tableros conectados al mismo tiempo.
        Esta es la función que se registra con 'aggregator.subscribe(...)' en main.py."""
        if not self._connections:
            return
        #Convierte el snapshot a JSON para enviarlo a los clientes WebSocket.
        payload = snapshot.model_dump_json()
        #Hcae una copia de la lista bajo el candado para iterar de forma segura
        async with self._lock:
            connections = list(self._connections)
        #Intenta enviar los datos a cada cliente
        dead: list[WebSocket] = [] #Lista de conexiones que fallaron
        for ws in connections:
            try:
                #Envia el JSON a traves del WebSocket. Si falla, se añade a la lista de muertos para desconectarlo.
                await ws.send_text(payload)
            except WebSocketDisconnect:
                dead.append(ws)
            except Exception as e:
                logger.warning("Error enviando a WebSocket: %s", e)
                dead.append(ws)
        #Desconecta todas las conexiones que fallaron
        for ws in dead:
            await self.disconnect(ws)

    async def send_to(
        self,
        websocket: WebSocket,
        data: str,
    ) -> None:
        #Envía datos a un WebSocket específico. Si falla, se desconecta la conexión.
        try:
            await websocket.send_text(data)
        except Exception as e:
            logger.warning("Error en send_to: %s", e)
            await self.disconnect(websocket)

    @property
    def connection_count(self) -> int:
        return len(self._connections)
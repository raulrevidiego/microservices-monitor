from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Optional
from uuid import uuid4

from collector.log_collector import collect_multiple_logs, collect_log_metrics
from collector.process_collector import collect_process_metrics
from core.models import MetricSnapshot, ServiceMetric

logger = logging.getLogger(__name__)

#Ficha de inofrmación de cada servicio que quiero monitorizar
class ServiceConfig:
    __slots__ = (
        "service_id",
        "service_name",
        "pids",
        "log_paths",
        "collect_interval",
    )

    def __init__(
        self,
        service_id: str,
        service_name: str,
        pids: Optional[list[int]] = None,
        log_paths: Optional[list[str]] = None,
        collect_interval: float = 5.0,
    ) -> None:
        self.service_id = service_id
        self.service_name = service_name
        self.pids = pids
        self.log_paths = log_paths or []
        self.collect_interval = collect_interval


#Aggregator principal, son 4 estructuras de datos internas con distintos roles 
class MetricsAggregator:

    def __init__(
        self,
        collect_interval: float = 5.0,
        history_size: int = 60,
    ) -> None:
        self._collect_interval = collect_interval
        self._history_size = history_size

        self._services: dict[str, ServiceConfig] = {} #es un dicccionario indexado por service_id, para actualizar o eliminar un servicio más facilmente
        self._history: list[MetricSnapshot] = [] #es una lista de snapshots que actúa como buffer circular (el dato mas nuevo expulsa al maqs viejo) de tamaño history_size, para que el dashboard genere un grafico
        self._subscribers: list[Callable[[MetricSnapshot], None]] = [] #la lista de callbacks que reciben cada snapshotr nuevo
        """El aggregator no sabe quee es WebSocket, solo tiene una lista de tareas pendientes en _suscribers.
        Cuando FastAPI monte el sistema de WebSockets para transmitir datos a la pantalla, se apuntará en esa lista.
        En cuanto el Aggregator tenga una foto nueva, recorrerá la lista y ejecutará esa función para "retransmitir" los datos en tiempo real al Dashboard."""
        self._running = False
        self._task: Optional[asyncio.Task] = None
        self._lock = asyncio.Lock()  
        """Cuando una tarea está guardando un dato en el historial, le pone el candado. Si otra tarea intenta entrar,
        el candado de asyncio lo suspende mientras termina la ultima"""

    #Registro de servicios

    def register_service(self, config: ServiceConfig) -> None:
        self._services[config.service_id] = config
        logger.info("Servicio registrado: %s", config.service_id)

    def unregister_service(self, service_id: str) -> None:
        self._services.pop(service_id, None)
        logger.info("Servicio eliminado: %s", service_id)

    #Suscriptores, esto sirve para detectar si el callback es una coroutine, si lo es ejecutar un await

    def subscribe(self, callback: Callable[[MetricSnapshot], None]) -> None:
        self._subscribers.append(callback)

    def unsubscribe(self, callback: Callable[[MetricSnapshot], None]) -> None:
        try:
            self._subscribers.remove(callback)
        except ValueError:
            pass

    # Historial
    @property
    def history(self) -> list[MetricSnapshot]:
        return list(self._history)

    def latest_snapshot(self) -> Optional[MetricSnapshot]:
        return self._history[-1] if self._history else None

    # Ciclo de vida, si ejecutase await directamente el codigo al que llama start se bloquearía eternamente, por eso create_task lanza _collect_loop, el bucle corre en paralelo

    async def start(self) -> None:
        if self._running:
            logger.warning("Aggregator ya está en marcha")
            return
        self._running = True
        self._task = asyncio.create_task(self._collect_loop())
        logger.info("Aggregator iniciado — intervalo: %.1fs", self._collect_interval)

    async def stop(self) -> None:
        self._running = False
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("Aggregator detenido")

    """Bucle principal.
    El corazón del sistema. El try/except Exception envuelve cada iteración completa,
    si la colección de un ciclo falla por cualquier razón, el error se loguea pero el bucle continúa en el siguiente ciclo. 
    Sin esto, una excepción no capturada en _collect_all mataría silenciosamente la tarea y el sistema dejaría de recoger métricas sin avisar.
    """
    async def _collect_loop(self) -> None:
        while self._running:
            try:
                snapshot = await self._collect_all()
                await self._store_snapshot(snapshot)
                await self._notify_subscribers(snapshot)
            except Exception as e:
                logger.error("Error en ciclo de colección: %s", e, exc_info=True)

            await asyncio.sleep(self._collect_interval)

    """Colección.
    Aquí está el beneficio real de asyncio. En lugar de recoger cada servicio en secuencia:
    primero servicio A (2s), luego servicio B (2s), luego servicio C (2s) = 6 segundos total, lanza todas las colecciones en paralelo con asyncio.gather y espera a que todas terminen. 
    Con 5 servicios el tiempo total es el del más lento, no la suma de todos.
    """
    async def _collect_all(self) -> MetricSnapshot:
        tasks = {
            service_id: asyncio.create_task(
                self._collect_service(config)
            )
            for service_id, config in self._services.items()
        }

        results = await asyncio.gather(*tasks.values(), return_exceptions=True)

        services: list[ServiceMetric] = []
        for service_id, result in zip(tasks.keys(), results):
            if isinstance(result, Exception):
                logger.error(
                    "Error colectando servicio %s: %s",
                    service_id, result, exc_info=True,
                )
                continue
            services.append(result)

        return MetricSnapshot(
            snapshot_id=str(uuid4()),
            collected_at=datetime.now(timezone.utc),
            services=services,
        )

    """Lanza en paralelo la colección de métricas de proceso y la de logs con otro gather interno. 
    Al final usa model_copy(update={"error_count": error_count}),el método de Pydantic para crear una copia modificada del modelo sin mutar el original. 
    Los modelos Pydantic son inmutables por defecto, que es exactamente lo que queremos en un sistema concurrente."""
    async def _collect_service(self, config: ServiceConfig) -> ServiceMetric:

        async def _empty_logs() -> tuple[int, list]:  # ✅ reemplaza asyncio.coroutine eliminado en Python 3.11
            return 0, []

        metrics_task = asyncio.create_task(
            collect_process_metrics(
                service_id=config.service_id,
                service_name=config.service_name,
                pids=config.pids,
            )
        )

        log_task = asyncio.create_task(
            collect_multiple_logs(
                log_paths=config.log_paths,
                service_id=config.service_id,
            )
            if config.log_paths
            else _empty_logs()
        )

        metrics, (error_count, _) = await asyncio.gather(metrics_task, log_task)

        return metrics.model_copy(update={"error_count": error_count})

    #Almacenamiento e histórico  ✅ indentación corregida (estaba con 2 espacios en vez de 4)
    async def _store_snapshot(self, snapshot: MetricSnapshot) -> None:
        async with self._lock:
            self._history.append(snapshot)
            if len(self._history) > self._history_size:
                self._history.pop(0)

    #Notificación
    async def _notify_subscribers(self, snapshot: MetricSnapshot) -> None:
        for callback in list(self._subscribers):
            try:
                result = callback(snapshot)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error("Error en suscriptor: %s", e, exc_info=True)
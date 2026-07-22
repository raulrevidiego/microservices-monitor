from __future__ import annotations

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes import router
from api.websockets import WebSocketManager
from core.aggregator import MetricsAggregator, ServiceConfig
from core.alert_engine import AlertEngine
from core.models import AlertRule, AlertLevel, MetricType

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger(__name__)

# ESTADO GLOBAL DE LA APLICACIÓN

aggregator = MetricsAggregator(collect_interval=5.0, history_size=60)
alert_engine = AlertEngine()
ws_manager = WebSocketManager()


def _setup_default_rules() -> None:
    alert_engine.add_rule(AlertRule(
        rule_id="cpu-warning",
        name="CPU alta",
        metric_type=MetricType.CPU,
        threshold=80.0, #Si la CPU supera el 80%.
        severity=AlertLevel.WARNING,
        cooldown_seconds=60, #Espera de 60 segundos.
    ))
    alert_engine.add_rule(AlertRule(
        rule_id="cpu-critical",
        name="CPU crítica",
        metric_type=MetricType.CPU,
        threshold=95.0, #Si la CPU supera el 95%.
        severity=AlertLevel.CRITICAL,
        cooldown_seconds=30, #Espera de 30 segundos.
    ))
    alert_engine.add_rule(AlertRule(
        rule_id="memory-warning",
        name="RAM alta",
        metric_type=MetricType.MEMORY,
        threshold=75.0, #Si la RAM supera el 75%.
        severity=AlertLevel.WARNING,
        cooldown_seconds=60, #Espera de 60 segundos.
    ))
    alert_engine.add_rule(AlertRule(
        rule_id="memory-critical",
        name="RAM crítica",
        metric_type=MetricType.MEMORY,
        threshold=90.0, #Si la RAM supera el 90%.
        severity=AlertLevel.CRITICAL,
        cooldown_seconds=30, #Espera de 30 segundos.
    ))
    alert_engine.add_rule(AlertRule(
        rule_id="latency-warning",
        name="Latencia alta",
        metric_type=MetricType.NETWORK,
        threshold=200.0, #Si la latencia supera los 200 ms.
        severity=AlertLevel.WARNING,
        cooldown_seconds=60, #Espera de 60 segundos.
    ))


def _setup_default_services() -> None:
    import os
    aggregator.register_service(ServiceConfig(
        service_id="local",
        service_name="Sistema local",
        pids=[os.getpid()],
        collect_interval=5.0,
    ))

# LIFESPAN
"""Esto es una función que controla todo lo que debe pasar al iniciar y al apagar el servidor.
FastAPI moderno usa lifespan en lugar del antiguo @app.on_event("startup").
Es un context manager asíncrono — todo el código antes del yield se ejecuta al arrancar, 
y todo lo de después al apagar. La ventaja es que el apagado está garantizado incluso si el servidor recibe una señal de interrupción, 
evitando dejar el aggregator corriendo en background sin control.
"""
@asynccontextmanager
async def lifespan(app: FastAPI):
    logger.info("Arrancando Monitor de Microservicios...")

    _setup_default_rules()
    _setup_default_services()

    aggregator.subscribe(alert_engine.process_snapshot)
    aggregator.subscribe(ws_manager.broadcast_snapshot)

    await aggregator.start()
    logger.info("Aggregator iniciado")
#Antes del yield se ejecuta al ARRANCAR 
    yield
#Depués del yield se ejecuta al APÂGAR
    logger.info("Apagando...")
    await aggregator.stop()
    await ws_manager.disconnect_all()
    logger.info("Apagado limpio completado")

# APLICACIÓN FASTAPI

app = FastAPI(
    title="Microservices Monitor",
    description="Monitor de microservicios con métricas en tiempo real",
    version="0.1.0",
    lifespan=lifespan,
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(router)
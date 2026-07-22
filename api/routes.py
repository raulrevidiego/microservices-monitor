from __future__ import annotations

import logging
from typing import Any

from fastapi import APIRouter, HTTPException, WebSocket, WebSocketDisconnect
from pydantic import BaseModel

logger = logging.getLogger(__name__)
router = APIRouter()

# IMPORTS TARDÍOS (evitan circular imports)


def _get_state() -> tuple[Any, Any, Any]:
    from api.main import aggregator, alert_engine, ws_manager
    return aggregator, alert_engine, ws_manager

# SCHEMAS DE RESPUESTA

class HealthResponse(BaseModel):
    status: str
    version: str
    services_monitored: int
    active_alerts: int
    ws_connections: int


class RuleCreateRequest(BaseModel):
    rule_id: str
    name: str
    metric_type: str
    threshold: float
    severity: str = "warning"
    cooldown_seconds: int = 60

# ENDPOINTS REST


@router.get("/health", response_model=HealthResponse)
async def health_check():
    aggregator, alert_engine, ws_manager = _get_state()
    snapshot = aggregator.latest_snapshot()
    return HealthResponse(
        status="ok",
        version="0.1.0",
        services_monitored=snapshot.service_count if snapshot else 0,
        active_alerts=len(alert_engine.active_events),
        ws_connections=ws_manager.connection_count,
    )


@router.get("/metrics/latest")
async def get_latest_metrics():
    aggregator, _, _ = _get_state()
    snapshot = aggregator.latest_snapshot()
    if snapshot is None:
        raise HTTPException(status_code=404, detail="Sin métricas todavía")
    return snapshot.model_dump()


@router.get("/metrics/history")
async def get_metrics_history(limit: int = 20):
    aggregator, _, _ = _get_state()
    history = aggregator.history[-limit:]
    return [s.model_dump() for s in history]


@router.get("/alerts/active")
async def get_active_alerts():
    _, alert_engine, _ = _get_state()
    return [e.model_dump() for e in alert_engine.active_events]


@router.get("/alerts/history")
async def get_alerts_history(limit: int = 50):
    _, alert_engine, _ = _get_state()
    return [e.model_dump() for e in alert_engine.event_history[-limit:]]


@router.get("/alerts/rules")
async def get_rules():
    _, alert_engine, _ = _get_state()
    return [r.model_dump() for r in alert_engine.rules]


@router.post("/alerts/rules")
async def create_rule(request: RuleCreateRequest):
    from core.models import AlertRule, AlertLevel, MetricType
    _, alert_engine, _ = _get_state()
    try:
        rule = AlertRule(
            rule_id=request.rule_id,
            name=request.name,
            metric_type=MetricType(request.metric_type),
            threshold=request.threshold,
            severity=AlertLevel(request.severity),
            cooldown_seconds=request.cooldown_seconds,
        )
        alert_engine.add_rule(rule)
        return {"status": "ok", "rule_id": rule.rule_id}
    except ValueError as e:
        raise HTTPException(status_code=422, detail=str(e))


@router.delete("/alerts/rules/{rule_id}")
async def delete_rule(rule_id: str):
    _, alert_engine, _ = _get_state()
    alert_engine.remove_rule(rule_id)
    return {"status": "ok", "rule_id": rule_id}

# WEBSOCKET

@router.websocket("/ws/metrics")
async def websocket_metrics(websocket: WebSocket):
    aggregator, _, ws_manager = _get_state()
    await ws_manager.connect(websocket)

    snapshot = aggregator.latest_snapshot()
    if snapshot:
        await ws_manager.send_to(websocket, snapshot.model_dump_json())

    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        await ws_manager.disconnect(websocket)
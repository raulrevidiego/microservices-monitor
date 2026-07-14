from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Optional
from uuid import uuid4

from core.models import (
    AlertEvent,
    AlertRule,
    AlertSeverity,
    AlertStatus,
    MetricSnapshot,
    MetricType,
    ServiceMetrics,
)

logger = logging.getLogger(__name__)

# EXTRACTOR DE VALORES
"""Esta función usa un dicccionario de lamdas como tabla dispatch en lugar de un bloque if/elif, esto permite añadir nuevos tipos de métricas sin modificar la función, 
solo añadiendo una nueva entrada al diccionario. También maneja errores de extracción y devuelve None si no se puede extraer el valor."""
def _extract_metric_value(
    metrics: ServiceMetrics,
    metric_type: MetricType,
) -> Optional[float]:   #Esta funcion recive 2 cosas, el paquete de metricas de un servicio y que tipo de metricas hay q buscar. Devuelve el numero decimal o nada si no lo encuentra
    extractors: dict[MetricType, Callable[[ServiceMetrics], float]] = { #Crea un diccionario lamdas para usar mas tarde
        MetricType.CPU: lambda m: m.cpu_percent,
        MetricType.MEMORY: lambda m: m.memory_percent,
        MetricType.NETWORK: lambda m: m.net_latency_ms if m.net_latency_ms is not None else 0.0,
        MetricType.DISK: lambda m: 0.0,
    }
    extractor = extractors.get(metric_type)
    if extractor is None:
        return None
    try:
        return extractor(metrics)
    except Exception as e:
        logger.debug("Error extrayendo métrica %s: %s", metric_type, e)
        return None

"""Construye el mensaje de alerta basado en la regla y los valores de métrica"""
def _build_alert_message(
    rule: AlertRule,
    metrics: ServiceMetrics,
    current_value: float,
) -> str:
    unit_map = {
        MetricType.CPU: "%",
        MetricType.MEMORY: "%",
        MetricType.NETWORK: "ms",
        MetricType.DISK: "%",
    }
    unit = unit_map.get(rule.metric_type, "")
    return (
        f"[{rule.severity.value.upper()}] {metrics.service_name} — "
        f"{rule.metric_type.value.upper()} al {current_value:.1f}{unit} "
        f"(umbral: {rule.threshold:.1f}{unit})"
    )


# COOLDOWN TRACKER, es una clase interna que gestiona cuando fué la última vez que unaq regla disparó una alerta para un servicio concreto.

class _CooldownTracker:
    def __init__(self) -> None:
        self._last_fired: dict[str, datetime] = {}

    def is_on_cooldown(self, key: str, cooldown_seconds: int) -> bool: #Si no han pasado los 60 seg devuelve in true 
        last = self._last_fired.get(key)
        if last is None:
            return False
        elapsed = (datetime.now(timezone.utc) - last).total_seconds()
        return elapsed < cooldown_seconds

    def mark_fired(self, key: str) -> None:
        self._last_fired[key] = datetime.now(timezone.utc)

    def clear(self, key: str) -> None:
        self._last_fired.pop(key, None)


# ─────────────────────────────────────────
# ALERT ENGINE
# ─────────────────────────────────────────

class AlertEngine:

    def __init__(self) -> None:
        self._rules: dict[str, AlertRule] = {}
        self._active_events: dict[str, AlertEvent] = {}
        self._event_history: list[AlertEvent] = []
        self._cooldown = _CooldownTracker()
        self._notifiers: list[Callable[[AlertEvent], None]] = []
        self._lock = asyncio.Lock()

    # ── Gestión de reglas ─────────────────

    def add_rule(self, rule: AlertRule) -> None:
        self._rules[rule.rule_id] = rule
        logger.info(
            "Regla añadida: %s — %s > %.1f (%s)",
            rule.name, rule.metric_type.value, rule.threshold, rule.severity.value,
        )

    def remove_rule(self, rule_id: str) -> None:
        self._rules.pop(rule_id, None)

    def enable_rule(self, rule_id: str) -> None:
        if rule_id in self._rules:
            self._rules[rule_id] = self._rules[rule_id].model_copy(
                update={"enabled": True}
            )

    def disable_rule(self, rule_id: str) -> None:
        if rule_id in self._rules:
            self._rules[rule_id] = self._rules[rule_id].model_copy(
                update={"enabled": False}
            )

    # ── Notificadores ─────────────────────

    def add_notifier(self, callback: Callable[[AlertEvent], None]) -> None:
        self._notifiers.append(callback)

    def remove_notifier(self, callback: Callable[[AlertEvent], None]) -> None:
        try:
            self._notifiers.remove(callback)
        except ValueError:
            pass

    # ── Estado ────────────────────────────

    @property
    def active_events(self) -> list[AlertEvent]:
        return list(self._active_events.values())

    @property
    def event_history(self) -> list[AlertEvent]:
        return list(self._event_history)

    @property
    def rules(self) -> list[AlertRule]:
        return list(self._rules.values())

    # ── Procesamiento de snapshots ─────────

    async def process_snapshot(self, snapshot: MetricSnapshot) -> list[AlertEvent]:
        fired: list[AlertEvent] = []

        for service_metrics in snapshot.services:
            for rule in self._rules.values():
                if not rule.enabled:
                    continue

                event = await self._evaluate_rule(rule, service_metrics)
                if event:
                    fired.append(event)

        await self._resolve_stale_events(snapshot)
        return fired

    async def _evaluate_rule(
        self,
        rule: AlertRule,
        metrics: ServiceMetrics,
    ) -> Optional[AlertEvent]:

        current_value = _extract_metric_value(metrics, rule.metric_type)
        if current_value is None:
            return None

        cooldown_key = f"{rule.rule_id}:{metrics.service_id}"

        if current_value > rule.threshold:
            if self._cooldown.is_on_cooldown(cooldown_key, rule.cooldown_seconds):
                return None

            event = AlertEvent(
                event_id=str(uuid4()),
                rule_id=rule.rule_id,
                service_id=metrics.service_id,
                service_name=metrics.service_name,
                metric_type=rule.metric_type,
                current_value=round(current_value, 2),
                threshold=rule.threshold,
                severity=rule.severity,
                status=AlertStatus.ACTIVE,
                triggered_at=datetime.now(timezone.utc),
                message=_build_alert_message(rule, metrics, current_value),
            )

            async with self._lock:
                self._active_events[cooldown_key] = event
                self._event_history.append(event)
                if len(self._event_history) > 500:
                    self._event_history.pop(0)

            self._cooldown.mark_fired(cooldown_key)
            await self._dispatch_notifiers(event)

            logger.warning(event.message)
            return event

        else:
            await self._try_resolve(cooldown_key, metrics)
            return None

    async def _try_resolve(self, cooldown_key: str, metrics: ServiceMetrics) -> None:
        async with self._lock:
            active = self._active_events.get(cooldown_key)
            if active is None:
                return

            resolved = active.model_copy(update={
                "status": AlertStatus.RESOLVED,
                "resolved_at": datetime.now(timezone.utc),
            })
            self._active_events.pop(cooldown_key)
            self._event_history.append(resolved)

        self._cooldown.clear(cooldown_key)
        logger.info(
            "Alerta resuelta: %s — %s",
            metrics.service_name, active.metric_type.value,
        )

    async def _resolve_stale_events(self, snapshot: MetricSnapshot) -> None:
        current_service_ids = {s.service_id for s in snapshot.services}

        async with self._lock:
            stale_keys = [
                key for key in self._active_events
                if key.split(":")[1] not in current_service_ids
            ]

        for key in stale_keys:
            parts = key.split(":", 1)
            if len(parts) == 2:
                service_id = parts[1]
                fake_metrics = ServiceMetrics(
                    service_id=service_id,
                    service_name=service_id,
                    cpu_percent=0.0,
                    memory_mb=0.0,
                    memory_percent=0.0,
                    net_bytes_sent=0,
                    net_bytes_recv=0,
                )
                await self._try_resolve(key, fake_metrics)

    async def _dispatch_notifiers(self, event: AlertEvent) -> None:
        for notifier in list(self._notifiers):
            try:
                result = notifier(event)
                if asyncio.iscoroutine(result):
                    await result
            except Exception as e:
                logger.error("Error en notificador: %s", e, exc_info=True)
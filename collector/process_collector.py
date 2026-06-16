from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4

import psutil

from core.models import ProcessInfo, ServiceMetrics

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────
# FUNCIONES AUXILIARES
# ─────────────────────────────────────────

def get_process_info(pid: int) -> Optional[ProcessInfo]:
    try:
        proc = psutil.Process(pid)

        with proc.oneshot():
            name = proc.name()
            status = proc.status()
            cpu = proc.cpu_percent()
            mem_bytes = proc.memory_info().rss
            mem_mb = mem_bytes / (1024 * 1024)

        return ProcessInfo(
            pid=pid,
            name=name,
            cpu_percent=cpu,
            memory_mb=round(mem_mb, 2),
            status=status,
        )

    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
        logger.debug("Proceso %s no disponible: %s", pid, e)
        return None


def _aggregate_cpu(processes: list[ProcessInfo]) -> float:
    total = sum(p.cpu_percent for p in processes)
    cpu_count = psutil.cpu_count(logical=True) or 1
    normalized = total / cpu_count
    return round(min(normalized, 100.0), 2)


def _aggregate_memory(processes: list[ProcessInfo]) -> tuple[float, float]:
    total_mb = sum(p.memory_mb for p in processes)
    total_ram = psutil.virtual_memory().total / (1024 * 1024)
    mem_percent = (total_mb / total_ram) * 100 if total_ram > 0 else 0.0
    return round(total_mb, 2), round(mem_percent, 2)


def _get_network_counters() -> tuple[int, int]:
    counters = psutil.net_io_counters()
    if counters is None:
        return 0, 0
    return counters.bytes_sent, counters.bytes_recv


# ─────────────────────────────────────────
# FUNCIÓN PRINCIPAL DEL COLECTOR
# ─────────────────────────────────────────

async def collect_process_metrics(
    service_id: str,
    service_name: str,
    pids: Optional[list[int]] = None,
) -> ServiceMetrics:

    if pids is None:
        all_pids = psutil.pids()
    else:
        all_pids = pids

    loop = asyncio.get_running_loop()
    raw_processes = await loop.run_in_executor(
        None,
        lambda: [get_process_info(pid) for pid in all_pids],
    )

    processes = [p for p in raw_processes if p is not None]

    if not processes:
        return ServiceMetrics(
            service_id=service_id,
            service_name=service_name,
            cpu_percent=0.0,
            memory_mb=0.0,
            memory_percent=0.0,
            net_bytes_sent=0,
            net_bytes_recv=0,
            processes=[],
        )

    cpu = _aggregate_cpu(processes)
    mem_mb, mem_percent = _aggregate_memory(processes)
    bytes_sent, bytes_recv = await loop.run_in_executor(None, _get_network_counters)

    return ServiceMetrics(
        service_id=service_id,
        service_name=service_name,
        timestamp=datetime.now(timezone.utc),
        cpu_percent=cpu,
        memory_mb=mem_mb,
        memory_percent=mem_percent,
        net_bytes_sent=bytes_sent,
        net_bytes_recv=bytes_recv,
        processes=processes,
    )
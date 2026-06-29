from __future__ import annotations
import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional
from uuid import uuid4
import psutil
from core.models import ProcessInfo, ServiceMetric
logger = logging.getLogger(__name__)

# FUNCIONES AUXILIARES, esta función convierte un PID del sitema operativo en un ProcessInfo validado o devuelve un None si el proceso ya no existe
def get_process_info(pid: int) -> Optional[ProcessInfo]:
    try:
        proc = psutil.Process(pid)

        with proc.oneshot(): #Esto es para hacer una única llamada al sistema operativo
            name = proc.name()
            status = proc.status()
            cpu = proc.cpu_percent()
            mem_bytes = proc.memory_info().rss
            mem_mb = mem_bytes / (1024 * 1024)

        return ProcessInfo(
            pid=pid,
            name=name,
            cpu_usage=cpu,
            memory_usage=round(mem_mb, 2),
            status=status,
        )

    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
        logger.debug("Proceso %s no disponible: %s", pid, e)
        return None #Son los 3 casos posibles NoSuchProcess, AccessDenied y ZombieProcess, en todos los casos devolvemos None porque el proceso no está disponible.


def _aggregate_cpu(processes: list[ProcessInfo]) -> float: #Esta función normaliza los datos para que no haya errores en función de los nucleos de la CPU 
    total = sum(p.cpu_usage for p in processes)
    cpu_count = psutil.cpu_count(logical=True) or 1
    normalized = total / cpu_count
    return round(min(normalized, 100.0), 2)


def _aggregate_memory(processes: list[ProcessInfo]) -> tuple[float, float]: #Devuelve una tupla (megabytes totales, porcentaje de memoria total) para no llamar a psutil 2 veces
    total_mb = sum(p.memory_usage for p in processes)
    total_ram = psutil.virtual_memory().total / (1024 * 1024)
    mem_percent = (total_mb / total_ram) * 100 if total_ram > 0 else 0.0
    return round(total_mb, 2), round(mem_percent, 2)


def _get_network_counters() -> tuple[int, int]:
    counters = psutil.net_io_counters()
    if counters is None:
        return 0, 0
    return counters.bytes_sent, counters.bytes_recv

# FUNCIÓN PRINCIPAL DEL COLECTOR
#Esta es la única función asíncrona del colector. El motivo es que psutil.pids() y los bucles de lectura podríam tardar y romper el bucle
async def collect_process_metrics(
    service_id: str,
    service_name: str,
    pids: Optional[list[int]] = None,
) -> ServiceMetric:

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
        return ServiceMetric(
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
    """Esto resuelve el problema, manda la operación bloqueante a threadpool
    y devuelve el control al event loop. El None le dice a asyncio que use su executor por defecto"""
    return ServiceMetric(
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
#Funcion main de testeo
"""
if __name__ == "__main__":
    import asyncio

    async def main():
        metrics = await collect_process_metrics(
            service_id="test-01",
            service_name="Test Local",
        )
        print(f"CPU: {metrics.cpu_percent}%")
        print(f"Memoria: {metrics.memory_mb} MB ({metrics.memory_percent}%)")
        print(f"Procesos capturados: {len(metrics.processes)}")
        print(f"Red enviado: {metrics.net_bytes_sent} bytes")
        print(f"Red recibido: {metrics.net_bytes_recv} bytes")

    asyncio.run(main())
"""
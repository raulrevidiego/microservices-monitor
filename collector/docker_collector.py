from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from typing import Optional

import docker
import docker.errors
from docker.models.containers import Container

from core.models import ProcessInfo, ServiceMetric

logger = logging.getLogger(__name__)

# CLIENTE DOCKER, creamos un clientes que se conecta al socket de Docker

def _get_docker_client() -> Optional[docker.DockerClient]:
    try:
        client = docker.from_env()
        client.ping()
        return client
    except docker.errors.DockerException as e:
        logger.warning("Docker no disponible: %s", e)
        return None
#El client.ping cerififca que el daemon está realmente disponible, si no devulve un none

# PARSEO DE ESTADÍSTICAS, docker no te da el porcentaje de CPU, asi que hay que calcularlo. 

def _parse_cpu_percent(stats: dict) -> float:
    cpu_delta = (
        stats["cpu_stats"]["cpu_usage"]["total_usage"]
        - stats["precpu_stats"]["cpu_usage"]["total_usage"]
    )
    system_delta = (
        stats["cpu_stats"].get("system_cpu_usage", 0)
        - stats["precpu_stats"].get("system_cpu_usage", 0)
    )
    num_cpus = stats["cpu_stats"].get("online_cpus") or len(
        stats["cpu_stats"]["cpu_usage"].get("percpu_usage", [1])
    )
    if system_delta <= 0 or cpu_delta < 0:
        return 0.0
    return round((cpu_delta / system_delta) * num_cpus * 100.0, 2) #El estado actual del CPU y el del ciclo anterior, num_cpus es para normalizar.

#Hay que restar la memoria caché para obtener la memroria real.
def _parse_memory(stats: dict) -> tuple[float, float]:
    mem_stats = stats.get("memory_stats", {})
    usage_bytes = mem_stats.get("usage", 0)
    cache = mem_stats.get("stats", {}).get("cache", 0)
    real_usage = max(usage_bytes - cache, 0)
    limit_bytes = mem_stats.get("limit", 1)

    mem_mb = round(real_usage / (1024 * 1024), 2)
    mem_percent = round((real_usage / limit_bytes) * 100.0, 2) if limit_bytes > 0 else 0.0
    return mem_mb, min(mem_percent, 100.0)

#Se suman los bytes de todas las interfaces de red.
def _parse_network(stats: dict) -> tuple[int, int]:
    networks = stats.get("networks", {})
    bytes_sent = sum(iface.get("tx_bytes", 0) for iface in networks.values())
    bytes_recv = sum(iface.get("rx_bytes", 0) for iface in networks.values())
    return bytes_sent, bytes_recv

#El container.top es como un ps, como abrir la terminal apra ver el administrador de tareas.
def _parse_processes(container: Container) -> list[ProcessInfo]:
    try:
        top = container.top()
        processes = []
        titles = top.get("Titles", [])
        pid_idx = next((i for i, t in enumerate(titles) if t == "PID"), 1) #Uso next porque dependiendo del SO cambia el diccionario que devuelve.
        cmd_idx = next((i for i, t in enumerate(titles) if t in ("CMD", "COMMAND")), -1) #Next hace una busqueda dinámica, busca los indices para PID, CMD, COMMAND

        for proc in top.get("Processes", []):
            try:
                pid = int(proc[pid_idx])
                name = proc[cmd_idx].split()[0] if cmd_idx >= 0 else "unknown"
                processes.append(ProcessInfo(
                    pid=pid,
                    name=name,
                    cpu_percent=0.0,
                    memory_mb=0.0,
                    status="running",
                ))
            except (ValueError, IndexError):
                continue
        return processes
    except docker.errors.APIError:
        return []

# COLECTOR POR CONTENEDOR
#Uso container.labels.get para obtener el nombre legible del servicio uando el contenedor forma parte de un stack de Docker Compose. Si no es un contenedor de Compose, usa el nombre del contenedor directamente.
def _collect_container_sync(container: Container) -> Optional[ServiceMetric]:
    try:
        stats = container.stats(stream=False)
        cpu = _parse_cpu_percent(stats)
        mem_mb, mem_percent = _parse_memory(stats)
        bytes_sent, bytes_recv = _parse_network(stats)
        processes = _parse_processes(container)

        service_id = container.name or container.short_id
        service_name = container.labels.get(
            "com.docker.compose.service",
            container.name or container.short_id,
        )

        return ServiceMetric(
            service_id=f"docker_{service_id}",
            service_name=service_name,
            timestamp=datetime.now(timezone.utc),
            cpu_percent=cpu,
            memory_mb=mem_mb,
            memory_percent=mem_percent,
            net_bytes_sent=bytes_sent,
            net_bytes_recv=bytes_recv,
            error_count=0,
            processes=processes,
        )

    except docker.errors.NotFound:
        logger.debug("Contenedor desaparecido durante la colección")
        return None
    except docker.errors.APIError as e:
        logger.warning("Error API Docker en contenedor %s: %s", container.name, e)
        return None
    except Exception as e:
        logger.error("Error inesperado en colector Docker: %s", e, exc_info=True)
        return None

# FUNCIÓN PRINCIPAL

async def collect_docker_metrics(
    container_names: Optional[list[str]] = None,
) -> list[ServiceMetric]:
    client = _get_docker_client()
    if client is None:
        return []

    try:
        if container_names:
            containers = []
            for name in container_names:
                try:
                    containers.append(client.containers.get(name))
                except docker.errors.NotFound:
                    logger.warning("Contenedor no encontrado: %s", name)
        else:
            containers = client.containers.list(filters={"status": "running"})

    except docker.errors.DockerException as e:
        logger.error("Error listando contenedores: %s", e)
        return []

    if not containers:
        logger.debug("Sin contenedores en ejecución")
        return []

    loop = asyncio.get_running_loop()

    tasks = [
        loop.run_in_executor(None, _collect_container_sync, container)
        for container in containers
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    metrics: list[ServiceMetric] = []
    for result in results:
        if isinstance(result, Exception):
            logger.error("Error colectando contenedor: %s", result)
            continue
        if result is not None:
            metrics.append(result)

    client.close()
    return metrics
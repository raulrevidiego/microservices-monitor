from __future__ import annotations

import asyncio
import logging
import re
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)
#Este modulo esta pensado para leer lineas de logs y detectar errores en los procesos que no notemos por los datos del hardware.

# ENUMS Y MODELOS INTERNOS
"""Varios niveles de log que podemos encontrar en los logs de los procesos. Se usa para clasificar la severidad de los mensajes de log."""
class LogLevel(str, Enum):
    DEBUG = "debug"
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"
    CRITICAL = "critical"

"""He usado una clase simple dado que los logs se cuentan en cientos o miles"""
class LogEntry:
    __slots__ = ("timestamp", "level", "message", "source") #La anotacion __slots__ se usa para optimizar el uso de memoria en la clase LogEntry, ya que evita la creación de un diccionario interno para cada instancia

    def __init__(
        self,
        timestamp: datetime,
        level: LogLevel,
        message: str,
        source: str,
    ) -> None:
        self.timestamp = timestamp
        self.level = level
        self.message = message
        self.source = source

    def __repr__(self) -> str:
        return f"<LogEntry {self.level.value} @ {self.timestamp.isoformat()}: {self.message[:60]}>"

# PATRONES DE DETECCIÓN
#Utilizo expresiones regulares validadas para comprobar si hay errores en el log, uso re.Pattern para compilar regex de una sola vez a nivel de módulo

_ERROR_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\b(ERROR|CRITICAL|FATAL)\b", re.IGNORECASE),
    re.compile(r"\bException\b", re.IGNORECASE),
    re.compile(r"\bTraceback\b", re.IGNORECASE),
    re.compile(r"\b(failed|failure)\b", re.IGNORECASE),
    re.compile(r"5\d{2}\b"),  # HTTP 5xx
]

_LEVEL_PATTERN = re.compile(
    r"\b(DEBUG|INFO|WARNING|WARN|ERROR|CRITICAL|FATAL)\b",
    re.IGNORECASE,
)

_TIMESTAMP_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}"),
    re.compile(r"\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}"),
]
"""Los patrones cubren cuatro categorías distintas de error: palabras clave explícitas (ERROR, CRITICAL, FATAL),
excepciones de Python (Exception, Traceback), palabras semánticas (failed, failure), y códigos HTTP de servidor (5xx).
Con esto capturamos tanto logs de aplicaciones Python como logs de nginx o cualquier servidor HTTP."""
# ─────────────────────────────────────────
# FUNCIONES AUXILIARES
# ─────────────────────────────────────────

def _is_error_line(line: str) -> bool:
    return any(pattern.search(line) for pattern in _ERROR_PATTERNS)


def _parse_level(line: str) -> LogLevel:
    match = _LEVEL_PATTERN.search(line)
    if not match:
        return LogLevel.INFO

    raw = match.group(1).upper()
    mapping = {
        "DEBUG": LogLevel.DEBUG,
        "INFO": LogLevel.INFO,
        "WARNING": LogLevel.WARNING,
        "WARN": LogLevel.WARNING,
        "ERROR": LogLevel.ERROR,
        "CRITICAL": LogLevel.CRITICAL,
        "FATAL": LogLevel.CRITICAL,
    }
    return mapping.get(raw, LogLevel.INFO)


def _parse_timestamp(line: str) -> datetime:
    for pattern in _TIMESTAMP_PATTERNS:
        match = pattern.search(line)
        if match:
            raw = match.group(0).replace("T", " ")
            for fmt in ("%Y-%m-%d %H:%M:%S", "%d/%m/%Y %H:%M:%S"):
                try:
                    return datetime.strptime(raw, fmt).replace(tzinfo=timezone.utc)
                except ValueError:
                    continue
    return datetime.now(timezone.utc)


def _parse_line(line: str, source: str) -> LogEntry:
    return LogEntry(
        timestamp=_parse_timestamp(line),
        level=_parse_level(line),
        message=line.strip(),
        source=source,
    )

#Esta función lee las últimas n líneas de un archivo de forma eficiente sin cargar todo el archivo en memoria. La técnica se llama "tail inverso"
def _read_last_lines(path: Path, n: int = 200) -> list[str]: 
    try:
        with path.open("rb") as f:
            f.seek(0, 2)
            file_size = f.tell()

            if file_size == 0:
                return []

            chunk_size = min(8192, file_size)
            chunks: list[bytes] = []
            lines_found = 0
            position = file_size

            while position > 0 and lines_found < n + 1:
                read_size = min(chunk_size, position)
                position -= read_size
                f.seek(position)
                chunk = f.read(read_size)
                chunks.append(chunk)
                lines_found += chunk.count(b"\n")

            content = b"".join(reversed(chunks))
            all_lines = content.decode("utf-8", errors="replace").splitlines()
            return all_lines[-n:]

    except (OSError, PermissionError) as e:
        logger.warning("No se puede leer %s: %s", path, e)
        return []

# FUNCIÓN PRINCIPAL
async def collect_log_metrics(
    log_path: str | Path,
    service_id: str,
    last_n_lines: int = 200,
) -> tuple[int, list[LogEntry]]:

    path = Path(log_path)

    if not path.exists():
        logger.debug("Log no encontrado: %s", path)
        return 0, []

    loop = asyncio.get_running_loop()
    lines = await loop.run_in_executor(
        None,
        lambda: _read_last_lines(path, last_n_lines),
    )

    entries: list[LogEntry] = []
    error_count = 0

    for line in lines:
        if not line.strip():
            continue

        entry = _parse_line(line, source=str(path))
        entries.append(entry)

        if _is_error_line(line):
            error_count += 1

    logger.debug(
        "Servicio %s — %d líneas leídas, %d errores detectados",
        service_id,
        len(entries),
        error_count,
    )

    return error_count, entries


async def collect_multiple_logs(
    log_paths: list[str | Path],
    service_id: str,
    last_n_lines: int = 200,
) -> tuple[int, list[LogEntry]]:

    tasks = [
        collect_log_metrics(path, service_id, last_n_lines)
        for path in log_paths
    ]

    results = await asyncio.gather(*tasks, return_exceptions=True)

    total_errors = 0
    all_entries: list[LogEntry] = []

    for result in results:
        if isinstance(result, Exception):
            logger.warning("Error recogiendo logs: %s", result)
            continue
        error_count, entries = result
        total_errors += error_count
        all_entries.extend(entries)

    all_entries.sort(key=lambda e: e.timestamp)
    return total_errors, all_entries

from __future__ import annotations 
"""Permite usar la funcion list[ProcessInfo]
sin que python vea como un error, ya que ProcessInfo se define despues de la clase
ProcessInfo, es decir, se puede usar antes de ser definida."""
from datetime import datetime, timezone
from typing import Optional
from enum import Enum
from pydantic import BaseModel, Field

"""ENUMS, sirve para definir str o int con un conjuto de valores predefinidos
esto evitara errores de escritura en el frontend y backend, si escribo warnin 
en vez de warning, el backend me lanzara un error, ya que no es un valor valido para
el enum AlertLevel"""

"""Modelo de métricas, aqui vemos un proceso del sistema operativo
y cogémos las métricas que nos inetresan de cada proceso"""
class MetricType(str, Enum):
    CPU = "CPU"
    Memory = "Memory"
    Disk = "Disk"
    Network = "Network"
class AlertLevel(str, Enum):
    INFO = "INFO"
    WARNING = "WARNING"
    CRITICAL = "CRITICAL"
class AlertStatus(str, Enum):
    ACTIVATE = "ACTIVE"
    RESOLVED = "RESOLVED"
    ACKNOWLEDGED = "ACKNOWLEDGED"
"""Con Field he limitado los valores de cada métrica a los que tienen sentido,
ge (greater than or equal) y le (less than or equal)"""
class ProcessInfo(BaseModel):
    pid: int #Process ID
    name: str #Process Name
    cpu_usage: float = Field(ge=0.0, le=100.0) #Porcentaje de uso de CPU
    memory_usage: float = Field(ge=0.0) #Porcentaje de uso de memoria
    status: str #Estado del proceso
"""Esta clase es para guardar el estado del servicio y sus métricas, es mas que nada por seguridad, ya que si el servicio se cae, podemos ver que métricas tenia antes de caerse"""
class ServiceMetric(BaseModel):
    service_id: str #define el id del servicio, es unico para cada servicio
    service_name: str #define un nombre legible
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc)) #guarda fecha y hora de medición
    #Metricas del servicio
    cpu_percent: float = Field(ge=0.0, le=100.0) 
    memory_mb: float = Field(ge=0.0)
    memory_percent: float = Field(ge=0.0, le=100.0)
    #Metricas de red
    net_bytes_sent: int = Field(ge=0) 
    net_bytes_recv: int = Field(ge=0)
    net_latency_ms: Optional[float] = None
    #Numero de errores y procesos
    error_count: int = Field(default=0, ge=0)
    processes: list[ProcessInfo] = Field(default_factory=list)
"""1"""
class MetricSnapshot(BaseModel):
    snapshot_id: str
    collected_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    services: list[ServiceMetric] = Field(default_factory=list)

    @property
    def service_count(self) -> int:
        return len(self.services)

    def get_service(self, service_id: str) -> Optional[ServiceMetric]:
        return next(
            (s for s in self.services if s.service_id == service_id),
            None
        )
"""AlelrtRule es la clase que define las reglas de alerta"""
class AlertRule(BaseModel):
    rule_id: str
    name: str
    metric_type: MetricType
    threshold: float = Field(gt=0.0)
    severity: AlertLevel = AlertLevel.WARNING
    enabled: bool = True
    cooldown_seconds: int = Field(default=60, ge=0) #Hay una espera entre notificicaciones de alerta de 60 seg

#Las alertas ya dispardas se guardan aquí
class AlertEvent(BaseModel):
    event_id: str
    rule_id: str
    service_id: str
    service_name: str
    metric_type: MetricType
    current_value: float
    threshold: float
    severity: AlertLevel = AlertLevel.WARNING
    status: AlertStatus = AlertStatus.ACTIVATE
    triggered_at: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    resolved_at: Optional[datetime] = None
    message: str = ""
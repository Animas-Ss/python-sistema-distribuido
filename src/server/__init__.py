"""Módulo del servidor distribuido."""
from .server import DistributedServer
from .tasks import TaskProcessor

__all__ = ["DistributedServer", "TaskProcessor"]

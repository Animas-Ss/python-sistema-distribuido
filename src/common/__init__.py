"""Módulo común de utilidades y protocolos compartidos."""
from .protocol import send_message, recv_message
from .constants import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_WORKERS,
    SOCKET_TIMEOUT,
    ACTION_EXECUTE_TASK,
    ACTION_BATCH_TASKS,
    ACTION_GET_STATUS,
    ACTION_PING,
    STATUS_SUCCESS,
    STATUS_ERROR,
)

__all__ = [
    "send_message",
    "recv_message",
    "DEFAULT_HOST",
    "DEFAULT_PORT",
    "DEFAULT_WORKERS",
    "SOCKET_TIMEOUT",
    "ACTION_EXECUTE_TASK",
    "ACTION_BATCH_TASKS",
    "ACTION_GET_STATUS",
    "ACTION_PING",
    "STATUS_SUCCESS",
    "STATUS_ERROR",
]

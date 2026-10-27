"""
Módulo cliente para la conexión y envío de tareas al servidor distribuido.
"""
import socket
import time
import uuid
from typing import Any, Dict, Optional

from src.common.constants import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    SOCKET_TIMEOUT,
    ACTION_EXECUTE_TASK,
    ACTION_GET_STATUS,
    ACTION_PING,
)
from src.common.protocol import recv_message, send_message


class DistributedClient:
    """Cliente para comunicarse con el servidor distribuido mediante sockets TCP."""

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT, timeout: float = SOCKET_TIMEOUT):
        self.host = host
        self.port = port
        self.timeout = timeout
        self.sock: Optional[socket.socket] = None

    def connect(self) -> bool:
        """Establece conexión TCP con el servidor."""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(self.timeout)
            self.sock.connect((self.host, self.port))
            return True
        except ConnectionRefusedError:
            print(f"[!] Error: No se pudo conectar a {self.host}:{self.port}. ¿Está el servidor encendido?")
            return False
        except Exception as e:
            print(f"[!] Error de conexión: {e}")
            return False

    def close(self):
        """Cierra el socket de conexión limpiamente."""
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None

    def send_task(self, task_type: str, params: Dict[str, Any], task_id: Optional[str] = None) -> Optional[dict]:
        """Envía una tarea individual al servidor y espera la respuesta del worker."""
        if not self.sock:
            print("[!] Socket no conectado.")
            return None

        if not task_id:
            task_id = f"task-{uuid.uuid4().hex[:6]}"

        message = {
            "action": ACTION_EXECUTE_TASK,
            "task_id": task_id,
            "task_type": task_type,
            "params": params
        }

        try:
            t0 = time.time()
            send_message(self.sock, message)
            response = recv_message(self.sock)
            round_trip = time.time() - t0
            if response:
                response["round_trip_sec"] = round(round_trip, 4)
            return response
        except Exception as e:
            print(f"[!] Error en comunicación con el servidor: {e}")
            return None

    def get_server_status(self) -> Optional[dict]:
        """Solicita el estado y métricas actuales del servidor."""
        if not self.sock:
            return None
        try:
            send_message(self.sock, {"action": ACTION_GET_STATUS})
            return recv_message(self.sock)
        except Exception as e:
            print(f"[!] Error consultando estado: {e}")
            return None

    def ping(self) -> bool:
        """Envía un ping al servidor para verificar conectividad inmediata."""
        if not self.sock:
            return False
        try:
            send_message(self.sock, {"action": ACTION_PING})
            res = recv_message(self.sock)
            return res is not None and res.get("status") == "PONG"
        except Exception:
            return False

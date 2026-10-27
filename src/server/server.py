"""
Servidor Distribuido con Sockets y Worker Pool (Pool de Hilos).
Recibe tareas enviadas por clientes a través de sockets TCP y las distribuye
a un pool de hilos de trabajo concurrentes (ThreadPoolExecutor).
"""
import concurrent.futures
import logging
import socket
import threading
import time
import uuid
from typing import Optional

from src.common.constants import (
    DEFAULT_HOST,
    DEFAULT_PORT,
    DEFAULT_WORKERS,
    ACTION_EXECUTE_TASK,
    ACTION_BATCH_TASKS,
    ACTION_GET_STATUS,
    ACTION_PING,
    STATUS_SUCCESS,
    STATUS_ERROR,
)
from src.common.protocol import recv_message, send_message
from src.server.tasks import TaskProcessor

# Configuración de Logging
logger = logging.getLogger("DistributedServer")


class DistributedServer:
    """
    Servidor que gestiona conexiones entrantes de clientes por sockets
    y despacha el procesamiento de tareas a un ThreadPoolExecutor de workers.
    """

    def __init__(self, host: str = DEFAULT_HOST, port: int = DEFAULT_PORT, max_workers: int = DEFAULT_WORKERS):
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self.server_socket: Optional[socket.socket] = None
        self.is_running = False

        # Pool de hilos para workers
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="WorkerThread"
        )

        # Métricas de rendimiento
        self.stats_lock = threading.Lock()
        self.tasks_processed = 0
        self.tasks_failed = 0
        self.active_tasks = 0

    def run_worker_task(self, task_data: dict) -> dict:
        """Función ejecutada por un hilo específico dentro del Worker Pool."""
        task_id = task_data.get("task_id", str(uuid.uuid4())[:8])
        task_type = task_data.get("task_type", "UNKNOWN")
        params = task_data.get("params", {})
        worker_name = threading.current_thread().name

        with self.stats_lock:
            self.active_tasks += 1

        logger.info(f"-> Worker '{worker_name}' inició tarea [{task_id}] (Tipo: {task_type})")
        start_time = time.time()

        try:
            result = TaskProcessor.execute(task_type, params)
            elapsed = time.time() - start_time
            with self.stats_lock:
                self.tasks_processed += 1
                self.active_tasks -= 1

            logger.info(f"<- Worker '{worker_name}' completó tarea [{task_id}] en {elapsed:.4f}s")
            return {
                "status": STATUS_SUCCESS,
                "task_id": task_id,
                "task_type": task_type,
                "worker": worker_name,
                "execution_time_sec": round(elapsed, 4),
                "result": result,
                "error": None
            }
        except Exception as e:
            elapsed = time.time() - start_time
            with self.stats_lock:
                self.tasks_failed += 1
                self.active_tasks -= 1

            logger.error(f"X- Worker '{worker_name}' falló en tarea [{task_id}]: {e}")
            return {
                "status": STATUS_ERROR,
                "task_id": task_id,
                "task_type": task_type,
                "worker": worker_name,
                "execution_time_sec": round(elapsed, 4),
                "result": None,
                "error": str(e)
            }

    def handle_client(self, client_sock: socket.socket, client_addr):
        """Maneja el ciclo de vida de la conexión con un cliente socket."""
        logger.info(f"Cliente conectado desde {client_addr}")
        try:
            while self.is_running:
                request = recv_message(client_sock)
                if request is None:
                    # Conexión cerrada limpiamente por el cliente
                    break

                action = request.get("action", ACTION_EXECUTE_TASK)

                if action == ACTION_EXECUTE_TASK:
                    # Tarea individual enviada al pool de workers
                    future = self.executor.submit(self.run_worker_task, request)
                    response = future.result()
                    send_message(client_sock, response)

                elif action == ACTION_BATCH_TASKS:
                    # Lote de tareas concurrentes
                    tasks = request.get("tasks", [])
                    futures = [self.executor.submit(self.run_worker_task, t) for t in tasks]
                    results = [f.result() for f in futures]
                    send_message(client_sock, {
                        "status": STATUS_SUCCESS,
                        "batch_size": len(results),
                        "results": results
                    })

                elif action == ACTION_GET_STATUS:
                    with self.stats_lock:
                        status_info = {
                            "status": "ONLINE",
                            "host": self.host,
                            "port": self.port,
                            "max_workers": self.max_workers,
                            "active_workers": self.active_tasks,
                            "tasks_completed": self.tasks_processed,
                            "tasks_failed": self.tasks_failed
                        }
                    send_message(client_sock, status_info)

                elif action == ACTION_PING:
                    send_message(client_sock, {"status": "PONG", "timestamp": time.time()})

                else:
                    send_message(client_sock, {
                        "status": STATUS_ERROR,
                        "error": f"Acción '{action}' no soportada por el servidor."
                    })

        except ConnectionResetError:
            logger.warning(f"Conexión reiniciada por el cliente {client_addr}")
        except Exception as e:
            logger.error(f"Error atendiendo a {client_addr}: {e}")
        finally:
            try:
                client_sock.close()
            except Exception:
                pass
            logger.info(f"Cliente desconectado: {client_addr}")

    def start(self):
        """Inicia el socket servidor y escucha conexiones entrantes."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(128)
            self.server_socket.settimeout(1.0)  # Permite responder a Ctrl+C en Windows
            self.is_running = True
            logger.info("=" * 68)
            logger.info(f" Servidor Distribuido iniciado en {self.host}:{self.port}")
            logger.info(f" Worker Pool configurado con {self.max_workers} hilos concurrentes")
            logger.info(" Presione Ctrl+C para detener el servidor")
            logger.info("=" * 68)

            while self.is_running:
                try:
                    client_sock, client_addr = self.server_socket.accept()
                except socket.timeout:
                    continue
                except (socket.error, OSError):
                    break

                client_handler = threading.Thread(
                    target=self.handle_client,
                    args=(client_sock, client_addr),
                    daemon=True,
                    name=f"ClientHandler-{client_addr[1]}"
                )
                client_handler.start()

        except KeyboardInterrupt:
            logger.info("Solicitud de detencion recibida (Ctrl+C)...")
        finally:
            self.stop()

    def stop(self):
        """Detiene el servidor y cierra el pool de workers."""
        self.is_running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
        logger.info("Cerrando Worker Pool...")
        self.executor.shutdown(wait=False, cancel_futures=True)
        logger.info("Servidor detenido correctamente.")

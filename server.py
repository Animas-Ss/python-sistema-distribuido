"""
Servidor Distribuido con Sockets y Worker Pool (Pool de Hilos).
Recibe tareas enviadas por clientes a través de sockets TCP y las distribuye
a un pool de hilos de trabajo concurrentes (ThreadPoolExecutor).
"""
import argparse
import concurrent.futures
import hashlib
import logging
import math
import os
import socket
import sys
import threading
import time
import uuid
from typing import Any, Dict

from protocol import recv_message, send_message

# Configuración de Logging con formato visual claro
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("DistributedServer")


class TaskProcessor:
    """Implementa la lógica de ejecución de tareas realizadas por los workers."""

    @staticmethod
    def is_prime(n: int) -> bool:
        if n < 2:
            return False
        if n in (2, 3):
            return True
        if n % 2 == 0 or n % 3 == 0:
            return False
        for i in range(5, int(math.isqrt(n)) + 1, 6):
            if n % i == 0 or n % (i + 2) == 0:
                return False
        return True

    @classmethod
    def execute(cls, task_type: str, params: Dict[str, Any]) -> Any:
        """Ejecuta una tarea según su tipo y parámetros."""
        if task_type == "PRIME_CHECK":
            number = int(params.get("number", 0))
            prime = cls.is_prime(number)
            return {"number": number, "is_prime": prime}

        elif task_type == "FACTORIAL":
            number = int(params.get("number", 0))
            if number < 0:
                raise ValueError("El factorial no está definido para números negativos.")
            if number > 20000:
                raise ValueError("Número demasiado grande para factorial (máx 20000).")
            # Para números grandes, calculamos el resultado o longitud de dígitos
            res = math.factorial(number)
            res_str = str(res)
            return {
                "number": number,
                "digits": len(res_str),
                "preview": res_str[:30] + ("..." if len(res_str) > 30 else "")
            }

        elif task_type == "HASH_DATA":
            text = str(params.get("text", ""))
            algorithm = params.get("algorithm", "sha256").lower()
            h = hashlib.new(algorithm)
            h.update(text.encode("utf-8"))
            return {"algorithm": algorithm, "hash": h.hexdigest(), "length": len(text)}

        elif task_type == "WORD_COUNT":
            text = str(params.get("text", ""))
            words = text.split()
            unique_words = len(set(w.lower() for w in words))
            return {
                "total_words": len(words),
                "unique_words": unique_words,
                "character_count": len(text)
            }

        elif task_type == "SIMULATE_PROCESSING":
            duration = float(params.get("duration", 1.0))
            label = params.get("label", "Job genérico")
            time.sleep(duration)
            return {"label": label, "simulated_seconds": duration}

        else:
            raise ValueError(f"Tipo de tarea desconocido: '{task_type}'")


class DistributedServer:
    def __init__(self, host: str = "0.0.0.0", port: int = 5000, max_workers: int = 5):
        self.host = host
        self.port = port
        self.max_workers = max_workers
        self.server_socket = None
        self.is_running = False

        # Pool de hilos para workers
        self.executor = concurrent.futures.ThreadPoolExecutor(
            max_workers=self.max_workers,
            thread_name_prefix="WorkerThread"
        )

        # Métricas del servidor
        self.stats_lock = threading.Lock()
        self.tasks_processed = 0
        self.tasks_failed = 0
        self.active_tasks = 0

    def run_worker_task(self, task_data: dict) -> dict:
        """Función ejecutada por un hilo del Worker Pool."""
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
                "status": "SUCCESS",
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
                "status": "ERROR",
                "task_id": task_id,
                "task_type": task_type,
                "worker": worker_name,
                "execution_time_sec": round(elapsed, 4),
                "result": None,
                "error": str(e)
            }

    def handle_client(self, client_sock: socket.socket, client_addr):
        """Maneja la sesión de comunicación con un cliente específico."""
        logger.info(f"Cliente conectado desde {client_addr}")
        try:
            while self.is_running:
                request = recv_message(client_sock)
                if request is None:
                    # Conexión cerrada limpiamente por el cliente
                    break

                action = request.get("action", "EXECUTE_TASK")

                if action == "EXECUTE_TASK":
                    # Tarea individual enviada al pool de workers
                    future = self.executor.submit(self.run_worker_task, request)
                    response = future.result()  # Espera resultado y responde
                    send_message(client_sock, response)

                elif action == "BATCH_TASKS":
                    # Lote de tareas concurrentes
                    tasks = request.get("tasks", [])
                    futures = [self.executor.submit(self.run_worker_task, t) for t in tasks]
                    results = [f.result() for f in futures]
                    send_message(client_sock, {
                        "status": "SUCCESS",
                        "batch_size": len(results),
                        "results": results
                    })

                elif action == "GET_SERVER_STATUS":
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

                elif action == "PING":
                    send_message(client_sock, {"status": "PONG", "timestamp": time.time()})

                else:
                    send_message(client_sock, {
                        "status": "ERROR",
                        "error": f"Acción '{action}' no soportada"
                    })

        except ConnectionResetError:
            logger.warning(f"Conexión reiniciada abruptamente por {client_addr}")
        except Exception as e:
            logger.error(f"Error atendiendo a {client_addr}: {e}")
        finally:
            try:
                client_sock.close()
            except Exception:
                pass
            logger.info(f"Cliente desconectado: {client_addr}")

    def start(self):
        """Inicia el servidor socket y el loop de aceptación de conexiones."""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

        try:
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(128)
            self.is_running = True
            logger.info("=" * 65)
            logger.info(f" Servidor Distribuido iniciado en {self.host}:{self.port}")
            logger.info(f" Worker Pool configurado con {self.max_workers} hilos concurrentes")
            logger.info(" Presione Ctrl+C para detener el servidor")
            logger.info("=" * 65)

            while self.is_running:
                try:
                    client_sock, client_addr = self.server_socket.accept()
                    # Cada conexión de cliente se atiende en un hilo dedicado
                    client_handler = threading.Thread(
                        target=self.handle_client,
                        args=(client_sock, client_addr),
                        daemon=True,
                        name=f"Client-{client_addr[1]}"
                    )
                    client_handler.start()
                except socket.error:
                    break

        except KeyboardInterrupt:
            logger.info("Solicitud de detención recibida (Ctrl+C)...")
        finally:
            self.stop()

    def stop(self):
        """Detiene el servidor y limpia los hilos."""
        self.is_running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
        logger.info("Cerrando Worker Pool...")
        self.executor.shutdown(wait=False, cancel_futures=True)
        logger.info("Servidor detenido correctamente.")


def main():
    parser = argparse.ArgumentParser(description="Servidor de Tareas Distribuidas con Sockets")
    parser.add_argument("--host", default="127.0.0.1", help="Dirección IP de escucha (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=5000, help="Puerto de escucha (default: 5000)")
    parser.add_argument("--workers", type=int, default=5, help="Cantidad de hilos en el pool de workers (default: 5)")
    args = parser.parse_args()

    server = DistributedServer(host=args.host, port=args.port, max_workers=args.workers)
    server.start()


if __name__ == "__main__":
    main()

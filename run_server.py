"""
Punto de entrada principal para el Servidor Distribuido.
Permite configurar el host, puerto y tamaño del worker pool por línea de comandos.
"""
import argparse
import logging
import sys

from src.common.constants import DEFAULT_HOST, DEFAULT_PORT, DEFAULT_WORKERS
from src.server.server import DistributedServer


def setup_logging():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] [%(threadName)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )


def main():
    parser = argparse.ArgumentParser(
        description="Servidor de Tareas Distribuidas con Sockets TCP y Worker Pool"
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"IP de escucha (default: {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Puerto de escucha (default: {DEFAULT_PORT})")
    parser.add_argument("--workers", type=int, default=DEFAULT_WORKERS, help=f"Hilos en el worker pool (default: {DEFAULT_WORKERS})")
    args = parser.parse_args()

    setup_logging()
    server = DistributedServer(host=args.host, port=args.port, max_workers=args.workers)
    try:
        server.start()
    except Exception as e:
        logging.error(f"Error fatal en el servidor: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

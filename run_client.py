"""
Punto de entrada principal para el Cliente del Sistema Distribuido.
Permite ejecutar en modo interactivo o ejecutar la suite de demostración automática.
"""
import argparse
import sys

from src.client.cli import run_demo_suite, run_interactive_mode
from src.client.client import DistributedClient
from src.common.constants import DEFAULT_HOST, DEFAULT_PORT


def main():
    parser = argparse.ArgumentParser(
        description="Cliente de Sistema Distribuido con Sockets TCP"
    )
    parser.add_argument("--host", default=DEFAULT_HOST, help=f"Host del servidor (default: {DEFAULT_HOST})")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT, help=f"Puerto del servidor (default: {DEFAULT_PORT})")
    parser.add_argument("--demo", action="store_true", help="Ejecutar suite de tareas de prueba automática")
    args = parser.parse_args()

    client = DistributedClient(host=args.host, port=args.port)
    if not client.connect():
        sys.exit(1)

    print(f"[+] Conectado exitosamente al servidor distribuido en {args.host}:{args.port}")
    try:
        if args.demo:
            run_demo_suite(client)
        else:
            run_interactive_mode(client)
    except KeyboardInterrupt:
        print("\n[*] Interrupción de usuario recibida.")
    finally:
        client.close()
        print("[*] Conexión cerrada.")


if __name__ == "__main__":
    main()

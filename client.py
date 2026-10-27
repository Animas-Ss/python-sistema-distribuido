"""
Cliente de Sistema Distribuido con Sockets.
Permite conectarse al servidor, enviar tareas individuales o en lote,
y visualizar los resultados procesados por los workers concurrentes.
"""
import argparse
import socket
import sys
import time
import uuid
from typing import Any, Dict, Optional

from protocol import recv_message, send_message


class DistributedClient:
    def __init__(self, host: str = "127.0.0.1", port: int = 5000):
        self.host = host
        self.port = port
        self.sock: Optional[socket.socket] = None

    def connect(self) -> bool:
        """Establece conexión TCP con el servidor."""
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.sock.settimeout(15.0)  # Timeout para evitar bloqueos eternos
            self.sock.connect((self.host, self.port))
            print(f"[+] Conectado exitosamente al servidor en {self.host}:{self.port}")
            return True
        except ConnectionRefusedError:
            print(f"[!] Error: No se pudo conectar a {self.host}:{self.port}. ¿Está el servidor encendido?")
            return False
        except Exception as e:
            print(f"[!] Error de conexión: {e}")
            return False

    def close(self):
        """Cierra el socket de conexión."""
        if self.sock:
            try:
                self.sock.close()
            except Exception:
                pass
            self.sock = None
            print("[*] Conexión cerrada.")

    def send_task(self, task_type: str, params: Dict[str, Any], task_id: Optional[str] = None) -> Optional[dict]:
        """Envía una tarea individual y espera el resultado."""
        if not self.sock:
            print("[!] Socket no conectado.")
            return None

        if not task_id:
            task_id = f"task-{uuid.uuid4().hex[:6]}"

        message = {
            "action": "EXECUTE_TASK",
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
            print(f"[!] Error al enviar/recibir tarea: {e}")
            return None

    def get_server_status(self) -> Optional[dict]:
        """Consulta el estado y estadísticas actuales del servidor."""
        if not self.sock:
            return None
        try:
            send_message(self.sock, {"action": "GET_SERVER_STATUS"})
            return recv_message(self.sock)
        except Exception as e:
            print(f"[!] Error al consultar estado: {e}")
            return None


def print_result_card(res: dict):
    """Muestra el resultado con formato visual estructurado."""
    status = res.get("status", "UNKNOWN")
    symbol = "✓" if status == "SUCCESS" else "✗"
    print("\n" + "-" * 55)
    print(f"[{symbol}] Tarea ID: {res.get('task_id')} | Tipo: {res.get('task_type')}")
    print(f"    Estado:        {status}")
    print(f"    Worker Hilo:   {res.get('worker')}")
    print(f"    Tiempo Worker: {res.get('execution_time_sec')} s")
    if "round_trip_sec" in res:
        print(f"    Latencia RTT:  {res.get('round_trip_sec')} s")
    if status == "SUCCESS":
        print(f"    Resultado:     {res.get('result')}")
    else:
        print(f"    Error:         {res.get('error')}")
    print("-" * 55)


def run_demo_suite(client: DistributedClient):
    """Ejecuta una batería de tareas representativas para probar el sistema."""
    print("\n=======================================================")
    print(" INICIANDO BATERÍA DE PRUEBAS DE TAREAS DISTRIBUIDAS")
    print("=======================================================")

    test_tasks = [
        ("PRIME_CHECK", {"number": 104729}, "Comprobación de número primo grande"),
        ("FACTORIAL", {"number": 500}, "Cálculo de factorial (500!)"),
        ("HASH_DATA", {"text": "Sistemas Distribuidos PFO 3 - Arquitectura Sockets", "algorithm": "sha256"}, "Hash SHA-256 criptográfico"),
        ("WORD_COUNT", {"text": "Nginx RabbitMQ PostgreSQL S3 Workers Pool Hilos Python Sockets"}, "Análisis y conteo de palabras"),
        ("SIMULATE_PROCESSING", {"duration": 1.2, "label": "Procesamiento de imagen simulado"}, "Simulación I/O intensivo"),
        ("PRIME_CHECK", {"number": 999999999989}, "Primo de 12 dígitos"),
    ]

    for task_type, params, desc in test_tasks:
        print(f"\n[>] Enviando: {desc}...")
        res = client.send_task(task_type, params)
        if res:
            print_result_card(res)
        time.sleep(0.2)

    # Consulta de estado final del servidor
    status = client.get_server_status()
    print("\n[+] Resumen de métricas del Servidor:")
    print(f"    - Estado:              {status.get('status')}")
    print(f"    - Workers en pool:     {status.get('max_workers')}")
    print(f"    - Tareas completadas:  {status.get('tasks_completed')}")
    print(f"    - Tareas fallidas:     {status.get('tasks_failed')}")


def run_interactive_mode(client: DistributedClient):
    """Menú interactivo por consola para el usuario."""
    while True:
        print("\n" + "=" * 45)
        print(" SISTEMA DISTRIBUIDO - MENÚ CLIENTE")
        print("=" * 45)
        print(" 1. Verificar si un número es primo")
        print(" 2. Calcular factorial de un número")
        print(" 3. Generar hash criptográfico (SHA-256)")
        print(" 4. Contar palabras de un texto")
        print(" 5. Simular procesamiento con delay")
        print(" 6. Ver estado y métricas del servidor")
        print(" 7. Ejecutar demo automática de tareas")
        print(" 0. Salir")
        print("=" * 45)

        choice = input("Seleccione una opción: ").strip()

        if choice == "1":
            raw = input("Ingrese número entero: ").strip()
            try:
                num = int(raw)
                res = client.send_task("PRIME_CHECK", {"number": num})
                if res:
                    print_result_card(res)
            except ValueError:
                print("[!] Error: debe ser un número entero válido.")

        elif choice == "2":
            raw = input("Ingrese número (ej. 200): ").strip()
            try:
                num = int(raw)
                res = client.send_task("FACTORIAL", {"number": num})
                if res:
                    print_result_card(res)
            except ValueError:
                print("[!] Error: debe ser un número entero válido.")

        elif choice == "3":
            text = input("Ingrese texto a hashear: ")
            algo = input("Algoritmo [sha256/md5/sha1] (default: sha256): ").strip() or "sha256"
            res = client.send_task("HASH_DATA", {"text": text, "algorithm": algo})
            if res:
                print_result_card(res)

        elif choice == "4":
            text = input("Ingrese texto para analizar: ")
            res = client.send_task("WORD_COUNT", {"text": text})
            if res:
                print_result_card(res)

        elif choice == "5":
            duration = input("Duración en segundos (ej. 1.5): ").strip() or "1.0"
            label = input("Etiqueta de la tarea: ").strip() or "Tarea personalizada"
            res = client.send_task("SIMULATE_PROCESSING", {"duration": float(duration), "label": label})
            if res:
                print_result_card(res)

        elif choice == "6":
            st = client.get_server_status()
            print("\n[+] Estado del Servidor:", st)

        elif choice == "7":
            run_demo_suite(client)

        elif choice == "0":
            print("[*] Saliendo del cliente.")
            break
        else:
            print("[!] Opción no válida.")


def main():
    parser = argparse.ArgumentParser(description="Cliente de Tareas Distribuidas con Sockets")
    parser.add_argument("--host", default="127.0.0.1", help="Host del servidor (default: 127.0.0.1)")
    parser.add_argument("--port", type=int, default=5000, help="Puerto del servidor (default: 5000)")
    parser.add_argument("--demo", action="store_true", help="Ejecuta la suite de demostración automática")
    args = parser.parse_args()

    client = DistributedClient(host=args.host, port=args.port)
    if not client.connect():
        sys.exit(1)

    try:
        if args.demo:
            run_demo_suite(client)
        else:
            run_interactive_mode(client)
    except KeyboardInterrupt:
        print("\n[*] Interrupción de usuario.")
    finally:
        client.close()


if __name__ == "__main__":
    main()

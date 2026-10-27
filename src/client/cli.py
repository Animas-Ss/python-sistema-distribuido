"""
Interfaz de línea de comandos (CLI) interactiva y demostraciones para el cliente.
"""
import time
from src.client.client import DistributedClient
from src.common.constants import STATUS_SUCCESS


def print_result_card(res: dict):
    """Muestra el resultado con formato visual legible y estructurado."""
    status = res.get("status", "UNKNOWN")
    symbol = "✓" if status == STATUS_SUCCESS else "✗"
    print("\n" + "-" * 60)
    print(f"[{symbol}] Tarea ID:     {res.get('task_id')} | Tipo: {res.get('task_type')}")
    print(f"    Estado:       {status}")
    print(f"    Worker Hilo:  {res.get('worker')}")
    print(f"    Tiempo Proc:  {res.get('execution_time_sec')} s")
    if "round_trip_sec" in res:
        print(f"    Latencia RTT: {res.get('round_trip_sec')} s")
    if status == STATUS_SUCCESS:
        print(f"    Resultado:    {res.get('result')}")
    else:
        print(f"    Error:        {res.get('error')}")
    print("-" * 60)


def run_demo_suite(client: DistributedClient):
    """Ejecuta una batería de tareas representativas para probar el sistema distribuido."""
    print("\n" + "=" * 60)
    print(" 🚀 INICIANDO DEMOSTRACIÓN DE TAREAS DISTRIBUIDAS")
    print("=" * 60)

    test_tasks = [
        ("PRIME_CHECK", {"number": 104729}, "Comprobación de número primo grande"),
        ("FACTORIAL", {"number": 500}, "Cálculo de factorial (500!)"),
        ("HASH_DATA", {"text": "Sistemas Distribuidos PFO 3 - Cliente/Servidor Sockets", "algorithm": "sha256"}, "Hash criptográfico SHA-256"),
        ("WORD_COUNT", {"text": "Nginx RabbitMQ PostgreSQL S3 Workers Pool Hilos Python Sockets"}, "Conteo y análisis de palabras"),
        ("SIMULATE_PROCESSING", {"duration": 1.2, "label": "Procesamiento de imagen simulado"}, "Simulación I/O intensivo con retardo"),
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
    if status:
        print("\n" + "=" * 60)
        print(" 📊 RESUMEN DE MÉTRICAS DEL SERVIDOR:")
        print(f"    • Estado:              {status.get('status')}")
        print(f"    • Host / Puerto:       {status.get('host')}:{status.get('port')}")
        print(f"    • Workers en Pool:     {status.get('max_workers')}")
        print(f"    • Tareas completadas:  {status.get('tasks_completed')}")
        print(f"    • Tareas fallidas:     {status.get('tasks_failed')}")
        print("=" * 60)


def run_interactive_mode(client: DistributedClient):
    """Menú interactivo por consola para el usuario."""
    while True:
        print("\n" + "=" * 50)
        print("   SISTEMA DISTRIBUIDO - MENÚ DEL CLIENTE")
        print("=" * 50)
        print(" 1. Verificar si un número es primo")
        print(" 2. Calcular factorial de un número")
        print(" 3. Generar hash criptográfico (SHA-256)")
        print(" 4. Contar palabras de un texto")
        print(" 5. Simular procesamiento con retardo")
        print(" 6. Ver estado y métricas del servidor")
        print(" 7. Ejecutar demo automática de tareas")
        print(" 0. Salir")
        print("=" * 50)

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
            try:
                res = client.send_task("SIMULATE_PROCESSING", {"duration": float(duration), "label": label})
                if res:
                    print_result_card(res)
            except ValueError:
                print("[!] Duración inválida.")

        elif choice == "6":
            st = client.get_server_status()
            print("\n[+] Estado del Servidor:", st)

        elif choice == "7":
            run_demo_suite(client)

        elif choice == "0":
            print("[*] Saliendo del cliente.")
            break
        else:
            print("[!] Opción no válida. Intente nuevamente.")

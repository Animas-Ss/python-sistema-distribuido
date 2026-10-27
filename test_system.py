"""
Suite de pruebas automatizadas para el Sistema Distribuido.
Verifica la comunicación por socket, concurrencia, distribución de hilos y manejo de errores.
"""
import concurrent.futures
import subprocess
import sys
import time
import unittest

from client import DistributedClient


class TestDistributedSystem(unittest.TestCase):
    server_process = None

    @classmethod
    def setUpClass(cls):
        """Inicia el servidor en un subproceso para las pruebas."""
        cls.server_process = subprocess.Popen(
            [sys.executable, "server.py", "--host", "127.0.0.1", "--port", "5005", "--workers", "4"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        time.sleep(1.0)  # Esperar a que el socket esté en escucha

    @classmethod
    def tearDownClass(cls):
        """Detiene el servidor."""
        if cls.server_process:
            cls.server_process.terminate()
            cls.server_process.wait(timeout=3)

    def test_single_task_prime_check(self):
        client = DistributedClient(host="127.0.0.1", port=5005)
        self.assertTrue(client.connect())
        res = client.send_task("PRIME_CHECK", {"number": 97})
        client.close()

        self.assertIsNotNone(res)
        self.assertEqual(res.get("status"), "SUCCESS")
        self.assertTrue(res["result"]["is_prime"])
        self.assertIn("WorkerThread", res["worker"])

    def test_concurrent_clients(self):
        """Prueba que múltiples clientes simultáneos sean atendidos por diferentes hilos del worker pool."""
        def client_worker(n):
            c = DistributedClient(host="127.0.0.1", port=5005)
            if not c.connect():
                return None
            res = c.send_task("SIMULATE_PROCESSING", {"duration": 0.2, "label": f"Client-{n}"})
            c.close()
            return res

        start_time = time.time()
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            futures = [executor.submit(client_worker, i) for i in range(4)]
            results = [f.result() for f in futures]
        total_time = time.time() - start_time

        # Con 4 workers y tareas de 0.2s en paralelo, debe tardar ~0.2-0.5s en total, no 0.8s
        self.assertLess(total_time, 0.9)
        workers_used = {r["worker"] for r in results if r}
        self.assertGreater(len(workers_used), 1, "Deberían utilizarse múltiples workers del pool")

    def test_error_handling(self):
        client = DistributedClient(host="127.0.0.1", port=5005)
        self.assertTrue(client.connect())
        # Tarea inválida
        res = client.send_task("INVALID_TASK_TYPE", {})
        self.assertEqual(res.get("status"), "ERROR")
        self.assertIn("desconocido", res.get("error"))

        # Factorial negativo
        res2 = client.send_task("FACTORIAL", {"number": -5})
        self.assertEqual(res2.get("status"), "ERROR")
        client.close()

    def test_server_status(self):
        client = DistributedClient(host="127.0.0.1", port=5005)
        self.assertTrue(client.connect())
        status = client.get_server_status()
        client.close()
        self.assertEqual(status.get("status"), "ONLINE")
        self.assertEqual(status.get("max_workers"), 4)
        self.assertGreater(status.get("tasks_completed"), 0)


if __name__ == "__main__":
    unittest.main()

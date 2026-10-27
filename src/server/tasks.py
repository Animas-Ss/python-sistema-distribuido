"""
Procesador de tareas ejecutadas por los hilos del Worker Pool.
"""
import hashlib
import math
import time
from typing import Any, Dict


class TaskProcessor:
    """Implementa la lógica de ejecución de tareas realizadas por los workers."""

    @staticmethod
    def is_prime(n: int) -> bool:
        """Determina de forma optimizada si un número es primo."""
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
                raise ValueError("Número demasiado grande para factorial (máximo permitido: 20000).")
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

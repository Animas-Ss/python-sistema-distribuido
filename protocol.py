"""
Módulo de protocolo de comunicación para el sistema distribuido.
Maneja la serialización JSON y el framing mediante prefijo de longitud (4 bytes, big-endian)
para evitar problemas de fragmentación y acumulación en flujos TCP.
"""
import json
import struct
from typing import Any, Optional

HEADER_FORMAT = "!I"  # Entero sin signo de 4 bytes (Network byte order / Big-endian)
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


def send_message(sock, message: dict) -> None:
    """Serializa un diccionario a JSON y lo envía con un encabezado de longitud."""
    payload = json.dumps(message, ensure_ascii=False).encode("utf-8")
    header = struct.pack(HEADER_FORMAT, len(payload))
    sock.sendall(header + payload)


def recv_all(sock, num_bytes: int) -> Optional[bytes]:
    """Lee exactamente num_bytes del socket o retorna None si se cierra la conexión."""
    buffer = bytearray()
    while len(buffer) < num_bytes:
        chunk = sock.recv(num_bytes - len(buffer))
        if not chunk:
            return None
        buffer.extend(chunk)
    return bytes(buffer)


def recv_message(sock) -> Optional[dict]:
    """Lee un mensaje completo respetando el encabezado de tamaño."""
    header = recv_all(sock, HEADER_SIZE)
    if not header:
        return None
    (payload_length,) = struct.unpack(HEADER_FORMAT, header)
    payload = recv_all(sock, payload_length)
    if payload is None:
        return None
    return json.loads(payload.decode("utf-8"))

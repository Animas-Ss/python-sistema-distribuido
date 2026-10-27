"""
Módulo de protocolo de comunicación para el sistema distribuido.
Maneja la serialización JSON y el framing mediante prefijo de longitud (4 bytes, big-endian)
para evitar problemas de fragmentación y acumulación en flujos TCP.
"""
import json
import struct
from typing import Optional

# Formato de cabecera: Entero sin signo de 4 bytes en Network Byte Order (Big-endian)
HEADER_FORMAT = "!I"
HEADER_SIZE = struct.calcsize(HEADER_FORMAT)


def send_message(sock, message: dict) -> None:
    """
    Serializa un diccionario a JSON y lo envía por el socket
    anteponiendo un encabezado de 4 bytes con la longitud del payload.
    """
    payload = json.dumps(message, ensure_ascii=False).encode("utf-8")
    header = struct.pack(HEADER_FORMAT, len(payload))
    sock.sendall(header + payload)


def recv_all(sock, num_bytes: int) -> Optional[bytes]:
    """
    Garantiza la lectura exacta de `num_bytes` desde el stream TCP.
    Si la conexión se cierra antes de completar los bytes requeridos, retorna None.
    """
    buffer = bytearray()
    while len(buffer) < num_bytes:
        chunk = sock.recv(num_bytes - len(buffer))
        if not chunk:
            return None
        buffer.extend(chunk)
    return bytes(buffer)


def recv_message(sock) -> Optional[dict]:
    """
    Lee un mensaje completo respetando el encabezado de tamaño.
    1. Lee 4 bytes de cabecera para determinar el tamaño.
    2. Lee exactamente la cantidad de bytes indicada por la cabecera.
    3. Deserializa el JSON a un diccionario Python.
    """
    header = recv_all(sock, HEADER_SIZE)
    if not header:
        return None
    (payload_length,) = struct.unpack(HEADER_FORMAT, header)
    payload = recv_all(sock, payload_length)
    if payload is None:
        return None
    return json.loads(payload.decode("utf-8"))

"""
Wrapper de compatibilidad para el protocolo de comunicación.
Redirige al módulo src.common.protocol.
"""
from src.common.protocol import HEADER_FORMAT, HEADER_SIZE, recv_all, recv_message, send_message

__all__ = ["HEADER_FORMAT", "HEADER_SIZE", "recv_all", "recv_message", "send_message"]

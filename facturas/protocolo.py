"""Protocolo de comunicación entre nodos: mensajes JSON con prefijo de longitud sobre TCP."""
import json
import socket
import struct
import threading

_HEADER = struct.Struct(">I")


def enviar(sock: socket.socket, mensaje: dict, lock: threading.Lock | None = None) -> None:
    datos = json.dumps(mensaje, separators=(",", ":")).encode("utf-8")
    trama = _HEADER.pack(len(datos)) + datos
    if lock is None:
        sock.sendall(trama)
    else:
        with lock:
            sock.sendall(trama)


def _leer_exacto(sock: socket.socket, n: int) -> bytes | None:
    buf = bytearray()
    while len(buf) < n:
        parte = sock.recv(n - len(buf))
        if not parte:
            return None
        buf.extend(parte)
    return bytes(buf)


def recibir(sock: socket.socket) -> dict | None:
    cabecera = _leer_exacto(sock, _HEADER.size)
    if cabecera is None:
        return None
    (longitud,) = _HEADER.unpack(cabecera)
    cuerpo = _leer_exacto(sock, longitud)
    if cuerpo is None:
        return None
    return json.loads(cuerpo.decode("utf-8"))

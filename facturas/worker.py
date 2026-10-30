"""Nodo worker: recibe facturas del coordinador y las procesa con un pool de hilos."""
import argparse
import os
import queue
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from .factura import procesar_factura
from .protocolo import enviar, recibir


class Worker:
    def __init__(self, nombre, host, puerto, hilos=2, retardo_io=0.02, fallar_tras=0, latido=0.5):
        self.nombre = nombre
        self.host, self.puerto = host, puerto
        self.hilos = hilos
        self.retardo_io = retardo_io
        self.fallar_tras = fallar_tras  # inyección de fallos: termina tras N facturas
        self.latido = latido
        self.pool = ThreadPoolExecutor(max_workers=hilos, thread_name_prefix=nombre)
        self.salida: queue.Queue = queue.Queue()
        self.procesadas = 0
        self._lock_cont = threading.Lock()

    def ejecutar(self):
        self.sock = socket.create_connection((self.host, self.puerto))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.lock = threading.Lock()
        enviar(self.sock, {"rol": "worker", "nombre": self.nombre, "capacidad": self.hilos}, self.lock)
        threading.Thread(target=self._latidos, daemon=True).start()
        threading.Thread(target=self._emisor, daemon=True).start()
        while True:
            msg = recibir(self.sock)
            if msg is None:
                break
            if msg["tipo"] == "trabajos":
                for t in msg["trabajos"]:
                    self.pool.submit(self._procesar, t)

    def _procesar(self, trabajo):
        r = procesar_factura(trabajo["xml"], self.retardo_io)
        r["job"] = trabajo["job"]
        self.salida.put(r)
        with self._lock_cont:
            self.procesadas += 1
            if self.fallar_tras and self.procesadas >= self.fallar_tras:
                print(f"{self.nombre}: fallo simulado tras {self.procesadas} facturas", flush=True)
                os._exit(1)

    def _emisor(self):
        """Agrupa los resultados disponibles y los envía en un solo mensaje."""
        while True:
            lote = [self.salida.get()]
            while True:
                try:
                    lote.append(self.salida.get_nowait())
                except queue.Empty:
                    break
            enviar(self.sock, {"tipo": "resultados", "resultados": lote}, self.lock)

    def _latidos(self):
        while True:
            time.sleep(self.latido)
            try:
                enviar(self.sock, {"tipo": "latido", "t": time.time()}, self.lock)
            except OSError:
                return


def main():
    p = argparse.ArgumentParser(description="Nodo worker")
    p.add_argument("--nombre", default=f"worker-{os.getpid()}")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--puerto", type=int, default=5000)
    p.add_argument("--hilos", type=int, default=2)
    p.add_argument("--retardo-io", type=float, default=0.02)
    p.add_argument("--fallar-tras", type=int, default=0)
    a = p.parse_args()
    Worker(a.nombre, a.host, a.puerto, a.hilos, a.retardo_io, a.fallar_tras).ejecutar()


if __name__ == "__main__":
    main()

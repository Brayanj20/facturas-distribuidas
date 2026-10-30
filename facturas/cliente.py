"""Cliente con Proxy remoto (patrón Proxy) hacia el coordinador."""
import argparse
import random
import socket
import threading
import time

from .factura import generar_factura
from .protocolo import enviar, recibir


class ProxyCoordinador:
    """Representante local del coordinador remoto.

    Oculta los sockets, la serialización y el envío por bloques: el cliente solo
    llama a enviar_facturas() y esperar(), como si el coordinador fuera un objeto local.
    """

    def __init__(self, host="127.0.0.1", puerto=5000, bloque=200):
        self.sock = socket.create_connection((host, puerto))
        self.sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
        self.lock = threading.Lock()
        self.bloque = bloque
        self.enviadas: dict[str, float] = {}
        self.resultados: dict[str, dict] = {}
        self.latencias: list[float] = []
        self._cv = threading.Condition()
        self._estado = None
        enviar(self.sock, {"rol": "cliente"}, self.lock)
        threading.Thread(target=self._receptor, daemon=True).start()

    def enviar_facturas(self, facturas: list[tuple[str, str]]):
        for i in range(0, len(facturas), self.bloque):
            parte = facturas[i:i + self.bloque]
            ahora = time.perf_counter()
            for jid, _ in parte:
                self.enviadas[jid] = ahora
            enviar(self.sock, {"tipo": "enviar", "trabajos": [{"job": j, "xml": x} for j, x in parte]}, self.lock)

    def esperar(self, cantidad: int, timeout: float = 120) -> bool:
        limite = time.monotonic() + timeout
        with self._cv:
            while len(self.resultados) < cantidad:
                restante = limite - time.monotonic()
                if restante <= 0:
                    return False
                self._cv.wait(restante)
        return True

    def estado(self) -> dict:
        with self._cv:
            self._estado = None
        enviar(self.sock, {"tipo": "estado"}, self.lock)
        with self._cv:
            self._cv.wait_for(lambda: self._estado is not None, timeout=5)
            return self._estado

    def _receptor(self):
        while True:
            msg = recibir(self.sock)
            if msg is None:
                return
            ahora = time.perf_counter()
            with self._cv:
                if msg["tipo"] == "resultados":
                    for r in msg["resultados"]:
                        if r["job"] not in self.resultados:
                            self.resultados[r["job"]] = r
                            self.latencias.append(ahora - self.enviadas[r["job"]])
                elif msg["tipo"] == "estado":
                    self._estado = msg
                self._cv.notify_all()

    def cerrar(self):
        self.sock.close()


def main():
    p = argparse.ArgumentParser(description="Cliente que envía facturas al coordinador")
    p.add_argument("--host", default="127.0.0.1")
    p.add_argument("--puerto", type=int, default=5000)
    p.add_argument("--facturas", type=int, default=100)
    a = p.parse_args()
    rng = random.Random(7)
    proxy = ProxyCoordinador(a.host, a.puerto)
    lote = [(f"job-{i}", generar_factura(i, rng)) for i in range(a.facturas)]
    t0 = time.perf_counter()
    proxy.enviar_facturas(lote)
    proxy.esperar(len(lote))
    dt = time.perf_counter() - t0
    validas = sum(r["estado"] == "valida" for r in proxy.resultados.values())
    print(f"{len(proxy.resultados)} facturas en {dt:.2f} s ({len(lote)/dt:.1f} fact/s); "
          f"válidas={validas}, rechazadas={len(lote)-validas}")
    por_worker = {}
    for r in proxy.resultados.values():
        por_worker[r["worker"]] = por_worker.get(r["worker"], 0) + 1
    print("Distribución por worker:", por_worker)
    proxy.cerrar()


if __name__ == "__main__":
    main()

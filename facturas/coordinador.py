"""Nodo coordinador (patrón Mediator).

Los workers y los clientes nunca se comunican entre sí: todo pasa por el coordinador,
que mantiene la cola de trabajos, asigna facturas a los workers según su capacidad
libre, vigila los latidos (heartbeat) y reasigna los trabajos de un worker caído.
"""
import argparse
import itertools
import socket
import sqlite3
import threading
import time
from collections import deque

from .observador import BusEventos, MonitorMetricas, RegistroConsola
from .protocolo import enviar, recibir


class RepositorioResultados:
    """Persistencia de resultados en SQLite (un solo hilo escritor)."""

    def __init__(self, ruta: str = ":memory:"):
        self._con = sqlite3.connect(ruta, check_same_thread=False)
        self._lock = threading.Lock()
        self._con.execute(
            "CREATE TABLE IF NOT EXISTS resultados (job TEXT PRIMARY KEY, factura TEXT, total REAL,"
            " categoria TEXT, estado TEXT, worker TEXT, intentos INTEGER)"
        )

    def guardar(self, filas: list[tuple]) -> None:
        with self._lock:
            self._con.executemany("INSERT OR REPLACE INTO resultados VALUES (?,?,?,?,?,?,?)", filas)
            self._con.commit()


class ConexionWorker:
    def __init__(self, wid, sock, capacidad):
        self.wid = wid
        self.sock = sock
        self.lock = threading.Lock()
        self.capacidad = capacidad
        self.en_curso: set[str] = set()
        self.ultimo_latido = time.monotonic()
        self.vivo = True

    @property
    def libres(self) -> int:
        return self.capacidad - len(self.en_curso)


class Coordinador:
    def __init__(self, puerto: int, lote: int = 1, timeout_latido: float = 1.5, db: str = ":memory:"):
        self.puerto = puerto
        self.lote = lote
        self.timeout_latido = timeout_latido
        self.eventos = BusEventos()
        self.metricas = MonitorMetricas()
        self.eventos.suscribir(RegistroConsola())
        self.eventos.suscribir(self.metricas)
        self.repo = RepositorioResultados(db)
        self._cv = threading.Condition()
        self._pendientes: deque[str] = deque()
        self._trabajos: dict[str, dict] = {}
        self._workers: dict[str, ConexionWorker] = {}
        self._clientes: dict[str, tuple[socket.socket, threading.Lock]] = {}
        self._ids = itertools.count(1)

    # ---------------- ciclo de vida ----------------
    def iniciar(self):
        srv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        srv.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        srv.bind(("0.0.0.0", self.puerto))
        srv.listen(64)
        threading.Thread(target=self._despachador, daemon=True).start()
        threading.Thread(target=self._vigilante_latidos, daemon=True).start()
        print(f"LISTO coordinador en puerto {self.puerto} (lote={self.lote})", flush=True)
        while True:
            conn, _ = srv.accept()
            conn.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
            threading.Thread(target=self._atender, args=(conn,), daemon=True).start()

    def _atender(self, conn: socket.socket):
        hola = recibir(conn)
        if not hola:
            conn.close()
            return
        if hola["rol"] == "worker":
            self._atender_worker(conn, hola)
        else:
            self._atender_cliente(conn)

    # ---------------- workers ----------------
    def _atender_worker(self, conn, hola):
        w = ConexionWorker(hola["nombre"], conn, int(hola["capacidad"]))
        with self._cv:
            self._workers[w.wid] = w
            self._cv.notify_all()
        self.eventos.publicar("worker_registrado", worker=w.wid, capacidad=w.capacidad)
        try:
            while True:
                msg = recibir(conn)
                if msg is None:
                    break
                if msg["tipo"] == "latido":
                    w.ultimo_latido = time.monotonic()
                elif msg["tipo"] == "resultados":
                    w.ultimo_latido = time.monotonic()
                    self._recibir_resultados(w, msg["resultados"])
        except OSError:
            pass
        self._worker_caido(w, "conexion cerrada")

    def _worker_caido(self, w: ConexionWorker, motivo: str):
        with self._cv:
            if not w.vivo:
                return
            w.vivo = False
            self._workers.pop(w.wid, None)
            reasignar = list(w.en_curso)
            w.en_curso.clear()
            for jid in reasignar:
                self._trabajos[jid]["intentos"] += 1
                self._pendientes.appendleft(jid)
            self._cv.notify_all()
        try:
            w.sock.close()
        except OSError:
            pass
        self.eventos.publicar("worker_caido", worker=w.wid, motivo=motivo)
        if reasignar:
            self.eventos.publicar("trabajos_reasignados", worker=w.wid, cantidad=len(reasignar))

    def _vigilante_latidos(self):
        while True:
            time.sleep(self.timeout_latido / 3)
            ahora = time.monotonic()
            for w in list(self._workers.values()):
                if ahora - w.ultimo_latido > self.timeout_latido:
                    self._worker_caido(w, "sin latido")

    def _hay_trabajo_y_capacidad(self) -> bool:
        return bool(self._pendientes) and any(w.libres > 0 for w in self._workers.values())

    def _despachador(self):
        """Asigna trabajos pendientes a los workers con capacidad libre."""
        while True:
            envios = []
            with self._cv:
                while not self._hay_trabajo_y_capacidad():
                    self._cv.wait()
                # reparto equitativo: el worker con más capacidad libre primero
                for w in sorted(self._workers.values(), key=lambda x: -x.libres):
                    if not self._pendientes:
                        break
                    n = min(w.libres, self.lote, len(self._pendientes))
                    if n <= 0:
                        continue
                    ids = [self._pendientes.popleft() for _ in range(n)]
                    w.en_curso.update(ids)
                    envios.append((w, ids))
            for w, ids in envios:
                trabajos = [{"job": j, "xml": self._trabajos[j]["xml"]} for j in ids]
                try:
                    enviar(w.sock, {"tipo": "trabajos", "trabajos": trabajos}, w.lock)
                    self.eventos.publicar("trabajos_asignados", worker=w.wid, cantidad=len(ids))
                except OSError:
                    self._worker_caido(w, "fallo al enviar")

    def _recibir_resultados(self, w: ConexionWorker, resultados: list[dict]):
        por_cliente: dict[str, list] = {}
        filas = []
        with self._cv:
            for r in resultados:
                jid = r["job"]
                w.en_curso.discard(jid)
                t = self._trabajos.pop(jid, None)
                if t is None:  # duplicado tras una reasignación
                    continue
                r["worker"] = w.wid
                r["intentos"] = t["intentos"]
                por_cliente.setdefault(t["cliente"], []).append(r)
                filas.append((jid, r["id"], r["total"], r["categoria"], r["estado"], w.wid, t["intentos"]))
            self._cv.notify_all()
        self.repo.guardar(filas)
        self.eventos.publicar("trabajos_completados", cantidad=len(filas))
        for cid, lista in por_cliente.items():
            sock, lock = self._clientes.get(cid, (None, None))
            if sock:
                try:
                    enviar(sock, {"tipo": "resultados", "resultados": lista}, lock)
                except OSError:
                    pass

    # ---------------- clientes ----------------
    def _atender_cliente(self, conn):
        cid = f"cliente-{next(self._ids)}"
        lock = threading.Lock()
        self._clientes[cid] = (conn, lock)
        self.eventos.publicar("cliente_conectado", cliente=cid)
        try:
            while True:
                msg = recibir(conn)
                if msg is None:
                    break
                if msg["tipo"] == "enviar":
                    with self._cv:
                        for t in msg["trabajos"]:
                            self._trabajos[t["job"]] = {"xml": t["xml"], "cliente": cid, "intentos": 1}
                            self._pendientes.append(t["job"])
                        self._cv.notify_all()
                    self.eventos.publicar("trabajos_recibidos", cantidad=len(msg["trabajos"]))
                elif msg["tipo"] == "estado":
                    with self._cv:
                        estado = {
                            "workers": {w.wid: w.capacidad for w in self._workers.values()},
                            "pendientes": len(self._pendientes),
                            "metricas": self.metricas.resumen(),
                        }
                    enviar(conn, {"tipo": "estado", **estado}, lock)
        except OSError:
            pass
        self._clientes.pop(cid, None)


def main():
    p = argparse.ArgumentParser(description="Nodo coordinador")
    p.add_argument("--puerto", type=int, default=5000)
    p.add_argument("--lote", type=int, default=1, help="trabajos por mensaje de asignación")
    p.add_argument("--timeout-latido", type=float, default=1.5)
    p.add_argument("--db", default=":memory:")
    a = p.parse_args()
    Coordinador(a.puerto, a.lote, a.timeout_latido, a.db).iniciar()


if __name__ == "__main__":
    main()

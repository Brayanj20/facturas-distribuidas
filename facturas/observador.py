"""Patrón Observer: bus de eventos del coordinador y suscriptores."""
import threading
import time
from collections import Counter


class BusEventos:
    """Sujeto observable. Los observadores implementan notificar(evento, datos)."""

    def __init__(self):
        self._observadores = []
        self._lock = threading.Lock()

    def suscribir(self, observador) -> None:
        with self._lock:
            self._observadores.append(observador)

    def publicar(self, evento: str, **datos) -> None:
        with self._lock:
            observadores = list(self._observadores)
        for obs in observadores:
            obs.notificar(evento, datos)


class RegistroConsola:
    """Observador que registra en consola los eventos relevantes."""

    RELEVANTES = {"worker_registrado", "worker_caido", "trabajos_reasignados", "cliente_conectado"}

    def __init__(self, detallado: bool = False):
        self.detallado = detallado

    def notificar(self, evento, datos):
        if self.detallado or evento in self.RELEVANTES:
            print(f"[{time.strftime('%H:%M:%S')}] {evento}: {datos}", flush=True)


class MonitorMetricas:
    """Observador que acumula métricas operativas del sistema."""

    def __init__(self):
        self.contadores = Counter()
        self._lock = threading.Lock()

    def notificar(self, evento, datos):
        with self._lock:
            self.contadores[evento] += datos.get("cantidad", 1)

    def resumen(self) -> dict:
        with self._lock:
            return dict(self.contadores)

"""Pruebas de escalabilidad, robustez y ajuste del sistema distribuido.

Lanza un coordinador y N workers como procesos independientes (nodos), envía
cargas de facturas mediante el ProxyCoordinador y mide throughput y latencias.
Resultados: resultados/metricas.csv, resultados/fallos.csv y gráficas PNG.
"""
import csv
import os
import random
import signal
import socket
import statistics
import subprocess
import sys
import time

from facturas.cliente import ProxyCoordinador
from facturas.factura import generar_factura

PY = sys.executable
SALIDA = os.path.join(os.path.dirname(os.path.abspath(__file__)), "resultados")


def puerto_libre():
    s = socket.socket()
    s.bind(("127.0.0.1", 0))
    p = s.getsockname()[1]
    s.close()
    return p


def lanzar(modulo, *args):
    return subprocess.Popen([PY, "-m", modulo, *map(str, args)], stdout=subprocess.PIPE,
                            stderr=subprocess.STDOUT, text=True)


def percentil(datos, p):
    datos = sorted(datos)
    k = (len(datos) - 1) * p / 100
    f = int(k)
    c = min(f + 1, len(datos) - 1)
    return datos[f] + (datos[c] - datos[f]) * (k - f)


def escenario(workers, facturas, hilos, lote, fallo=None, semilla=1):
    puerto = puerto_libre()
    coord = lanzar("facturas.coordinador", "--puerto", puerto, "--lote", lote, "--timeout-latido", 1.5)
    coord.stdout.readline()  # espera "LISTO"
    procs = []
    for i in range(workers):
        extra = []
        if fallo == "caida" and i == 0:
            extra = ["--fallar-tras", 40]
        procs.append(lanzar("facturas.worker", "--nombre", f"worker-{i+1}", "--puerto", puerto,
                            "--hilos", hilos, *extra))
    proxy = ProxyCoordinador(puerto=puerto)
    while len(proxy.estado()["workers"]) < workers:
        time.sleep(0.05)
    rng = random.Random(semilla)
    lote_facturas = [(f"job-{i}", generar_factura(i, rng)) for i in range(facturas)]
    t0 = time.perf_counter()
    proxy.enviar_facturas(lote_facturas)
    if fallo == "congelado":
        time.sleep(0.3)
        os.kill(procs[0].pid, signal.SIGSTOP)  # el worker deja de responder (sin latidos)
    ok = proxy.esperar(facturas, timeout=180)
    dt = time.perf_counter() - t0
    est = proxy.estado()
    m = est["metricas"]
    rechazadas = sum(r["estado"] == "rechazada" for r in proxy.resultados.values())
    fila = {
        "workers": workers, "facturas": facturas, "hilos_por_worker": hilos, "lote": lote,
        "fallo": fallo or "ninguno", "completadas": len(proxy.resultados), "ok": ok,
        "tiempo_s": round(dt, 3), "throughput_fps": round(len(proxy.resultados) / dt, 1),
        "lat_p50_ms": round(percentil(proxy.latencias, 50) * 1000, 1),
        "lat_p95_ms": round(percentil(proxy.latencias, 95) * 1000, 1),
        "lat_media_ms": round(statistics.mean(proxy.latencias) * 1000, 1),
        "rechazadas": rechazadas,
        "reasignadas": m.get("trabajos_reasignados", 0),
        "workers_caidos": m.get("worker_caido", 0),
        "mensajes_asignacion": m.get("trabajos_asignados", 0) and None,
    }
    proxy.cerrar()
    for p in procs:
        try:
            os.kill(p.pid, signal.SIGCONT)
        except ProcessLookupError:
            pass
        p.kill()
    coord.kill()
    for p in procs + [coord]:
        p.wait()
    del fila["mensajes_asignacion"]
    print(fila, flush=True)
    return fila


def guardar(nombre, filas):
    with open(os.path.join(SALIDA, nombre), "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(filas[0].keys()))
        w.writeheader()
        w.writerows(filas)


def graficas(base, ajuste):
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    fig, ax = plt.subplots(1, 2, figsize=(10, 4), dpi=150)
    for carga, marca in ((200, "o"), (800, "s")):
        f = [r for r in base if r["facturas"] == carga]
        ax[0].plot([r["workers"] for r in f], [r["throughput_fps"] for r in f], marker=marca,
                   label=f"{carga} facturas")
        ax[1].plot([r["workers"] for r in f], [r["lat_p95_ms"] for r in f], marker=marca,
                   label=f"{carga} facturas")
    ax[0].set(title="Throughput vs. número de workers", xlabel="Workers", ylabel="Facturas/s", xticks=[1, 2, 4])
    ax[1].set(title="Latencia p95 vs. número de workers", xlabel="Workers", ylabel="ms", xticks=[1, 2, 4])
    for a in ax:
        a.grid(alpha=.3)
        a.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(SALIDA, "escalabilidad.png"))

    fig, ax = plt.subplots(1, 2, figsize=(10, 4), dpi=150)
    etiquetas = [f"{r['workers']} w" for r in ajuste if r["config"] == "base"]
    x = range(len(etiquetas))
    b = [r for r in ajuste if r["config"] == "base"]
    a2 = [r for r in ajuste if r["config"] == "ajustado"]
    ax[0].bar([i - .2 for i in x], [r["throughput_fps"] for r in b], .4, label="Base (2 hilos, lote 1)")
    ax[0].bar([i + .2 for i in x], [r["throughput_fps"] for r in a2], .4, label="Ajustado (8 hilos, lote 8)")
    ax[1].bar([i - .2 for i in x], [r["lat_p95_ms"] for r in b], .4, label="Base")
    ax[1].bar([i + .2 for i in x], [r["lat_p95_ms"] for r in a2], .4, label="Ajustado")
    ax[0].set(title="Throughput antes y después del ajuste (800 facturas)", ylabel="Facturas/s",
              xticks=list(x), xticklabels=etiquetas)
    ax[1].set(title="Latencia p95 antes y después del ajuste", ylabel="ms", xticks=list(x), xticklabels=etiquetas)
    for a in ax:
        a.grid(alpha=.3, axis="y")
        a.legend()
    fig.tight_layout()
    fig.savefig(os.path.join(SALIDA, "ajuste.png"))


def main():
    os.makedirs(SALIDA, exist_ok=True)
    base = [escenario(w, n, hilos=2, lote=1) for n in (200, 800) for w in (1, 2, 4)]
    guardar("metricas_base.csv", base)
    ajuste = []
    for w in (1, 2, 4):
        r = dict(next(x for x in base if x["workers"] == w and x["facturas"] == 800))
        r["config"] = "base"
        ajuste.append(r)
        r2 = escenario(w, 800, hilos=8, lote=8)
        r2["config"] = "ajustado"
        ajuste.append(r2)
    guardar("metricas_ajuste.csv", ajuste)
    fallos = [escenario(4, 800, hilos=8, lote=8, fallo="caida"),
              escenario(4, 800, hilos=8, lote=8, fallo="congelado")]
    guardar("fallos.csv", fallos)
    graficas(base, ajuste)


if __name__ == "__main__":
    main()

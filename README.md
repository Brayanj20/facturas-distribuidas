# facturas-distribuidas

Prototipo académico de un **sistema distribuido de procesamiento de facturas electrónicas**
(Arquitectura de Software – Unidad 2, Actividad 4 «Tejiendo redes: arquitectura de software entre hilos y nodos»).

Un **nodo coordinador** recibe lotes de facturas XML de los clientes y las reparte entre **N nodos worker**.
Cada worker procesa las facturas en paralelo con un **pool de hilos** (parseo del XML, validación de
subtotal/IVA/total, clasificación por monto y simulación de la validación remota). Los resultados se
guardan en SQLite y se devuelven al cliente.

## Patrones de diseño

| Patrón | Dónde | Para qué |
|---|---|---|
| Mediator | `facturas/coordinador.py` (`Coordinador`) | Workers y clientes no se conocen entre sí; el coordinador centraliza la asignación |
| Proxy (remoto) | `facturas/cliente.py` (`ProxyCoordinador`) | El cliente usa el coordinador como un objeto local; oculta sockets y serialización |
| Observer | `facturas/observador.py` (`BusEventos`, `RegistroConsola`, `MonitorMetricas`) | Notificar eventos (worker caído, trabajos completados) a varios suscriptores |
| Thread Pool / Productor–Consumidor | `facturas/worker.py` | Concurrencia dentro de cada nodo y desacople entre procesamiento y envío |
| Heartbeat | `worker.py` / `coordinador.py` | Detección de nodos caídos y reasignación de trabajos |

## Requisitos

- Python 3.10 o superior. Solo biblioteca estándar para el sistema.
- `matplotlib` únicamente para generar las gráficas del benchmark (`pip install matplotlib`).

## Ejecución manual (cada comando en una terminal distinta)

```bash
python -m facturas.coordinador --puerto 5000 --lote 8
python -m facturas.worker --nombre worker-1 --puerto 5000 --hilos 8
python -m facturas.worker --nombre worker-2 --puerto 5000 --hilos 8
python -m facturas.cliente --puerto 5000 --facturas 500
```

Para simular un fallo: `python -m facturas.worker --nombre worker-3 --hilos 8 --fallar-tras 40`
(el worker termina tras 40 facturas y el coordinador reasigna sus trabajos pendientes).

Los workers pueden ejecutarse en otras máquinas usando `--host <IP del coordinador>`.

## Pruebas de desempeño

```bash
python benchmark.py      # escalabilidad (1, 2 y 4 workers), ajuste y fallos
python prueba_lote.py    # efecto aislado del tamaño de lote
```

Genera `resultados/*.csv`, `resultados/escalabilidad.png` y `resultados/ajuste.png`.

## Estructura

```
facturas/
  protocolo.py    # mensajes JSON con prefijo de longitud sobre TCP
  factura.py      # generación y procesamiento de facturas XML
  observador.py   # patrón Observer
  coordinador.py  # nodo coordinador (Mediator, despachador, latidos, SQLite)
  worker.py       # nodo worker (pool de hilos, emisor de resultados, latidos)
  cliente.py      # ProxyCoordinador y cliente de línea de comandos
benchmark.py, prueba_lote.py
docs/uml/         # diagramas de componentes, secuencia y despliegue
resultados/       # métricas medidas
```

Autor: Brayan Julián Herrera Rodríguez – Institución Tecnológica del Oriente, 2026.

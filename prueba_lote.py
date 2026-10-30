"""Experimento complementario: efecto aislado del tamaño de lote con alta concurrencia."""
import os
from benchmark import escenario, guardar, SALIDA

if __name__ == "__main__":
    os.makedirs(SALIDA, exist_ok=True)
    filas = []
    for lote in (1, 8, 32):
        for retardo_cero in (False,):
            r = escenario(4, 4000, hilos=32, lote=lote)
            filas.append(r)
    guardar("efecto_lote.csv", filas)

"""Generación y procesamiento de facturas electrónicas simuladas (XML tipo UBL simplificado)."""
import random
import time
import xml.etree.ElementTree as ET

IVA = 0.19


def generar_factura(numero: int, rng: random.Random, tasa_error: float = 0.05) -> str:
    raiz = ET.Element("Invoice")
    ET.SubElement(raiz, "ID").text = f"FE-{numero:06d}"
    ET.SubElement(raiz, "NITEmisor").text = str(rng.randint(800000000, 999999999))
    items = ET.SubElement(raiz, "Items")
    subtotal = 0.0
    for _ in range(rng.randint(1, 8)):
        cantidad = rng.randint(1, 20)
        precio = round(rng.uniform(5_000, 900_000), 2)
        item = ET.SubElement(items, "Item")
        ET.SubElement(item, "Cantidad").text = str(cantidad)
        ET.SubElement(item, "PrecioUnitario").text = f"{precio:.2f}"
        subtotal += cantidad * precio
    subtotal = round(subtotal, 2)
    iva = round(subtotal * IVA, 2)
    total = round(subtotal + iva, 2)
    if rng.random() < tasa_error:  # inyecta inconsistencias para validar el control
        total = round(total * rng.uniform(1.01, 1.2), 2)
    ET.SubElement(raiz, "Subtotal").text = f"{subtotal:.2f}"
    ET.SubElement(raiz, "IVA").text = f"{iva:.2f}"
    ET.SubElement(raiz, "Total").text = f"{total:.2f}"
    return ET.tostring(raiz, encoding="unicode")


def clasificar(total: float) -> str:
    if total < 1_000_000:
        return "menor"
    if total < 10_000_000:
        return "media"
    return "mayor"


def procesar_factura(xml: str, retardo_io: float = 0.02) -> dict:
    """Parsea, valida totales e IVA, clasifica y simula la validación remota (E/S)."""
    raiz = ET.fromstring(xml)
    calculado = sum(
        int(i.findtext("Cantidad")) * float(i.findtext("PrecioUnitario"))
        for i in raiz.iter("Item")
    )
    subtotal = float(raiz.findtext("Subtotal"))
    iva = float(raiz.findtext("IVA"))
    total = float(raiz.findtext("Total"))
    errores = []
    if abs(calculado - subtotal) > 0.05:
        errores.append("subtotal inconsistente")
    if abs(round(subtotal * IVA, 2) - iva) > 0.05:
        errores.append("IVA inconsistente")
    if abs(subtotal + iva - total) > 0.05:
        errores.append("total inconsistente")
    time.sleep(retardo_io)  # simula consulta al servicio de validación (operación de E/S)
    return {
        "id": raiz.findtext("ID"),
        "nit": raiz.findtext("NITEmisor"),
        "total": total,
        "categoria": clasificar(total),
        "estado": "valida" if not errores else "rechazada",
        "errores": errores,
    }

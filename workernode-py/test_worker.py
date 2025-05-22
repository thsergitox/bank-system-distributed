#!/usr/bin/env python3
"""
Test script para el Worker Node Python
Valida funcionalidad básica sin conexión al servidor Java
"""

import os
import sys
import tempfile
import shutil
from nodo_trabajador import (
    NodoTrabajadorPython,
    JavaObjectTranslator,
    EstadoOperacion,
    TipoOperacion,
)


def test_file_operations():
    """Prueba las operaciones de archivo del worker"""
    print("=== Test de Operaciones de Archivo ===")

    # Crear directorio temporal
    test_dir = tempfile.mkdtemp(prefix="test_worker_")
    print(f"Directorio de prueba: {test_dir}")

    try:
        # Crear worker de prueba
        worker = NodoTrabajadorPython("test_worker", "localhost", 12346, 12351)
        worker.directorio_base_datos = test_dir

        # Crear datos de prueba
        test_data = {
            "CUENTA_P1": [
                {
                    "idCuenta": 100001,
                    "idCliente": 1,
                    "saldo": 5000.0,
                    "tipoCuenta": "Corriente",
                },
                {
                    "idCuenta": 100002,
                    "idCliente": 2,
                    "saldo": 3000.0,
                    "tipoCuenta": "Ahorros",
                },
                {
                    "idCuenta": 100003,
                    "idCliente": 3,
                    "saldo": 1500.0,
                    "tipoCuenta": "Corriente",
                },
            ]
        }

        # Probar guardado de datos
        print("Probando guardado de datos de partición...")
        if worker._guardar_datos_particiones(test_data):
            print("✓ Datos guardados correctamente")
        else:
            print("✗ Error guardando datos")
            return False

        # Probar lectura de saldo
        print("Probando lectura de saldo...")
        worker.particiones_asignadas = ["CUENTA_P1"]
        saldo = worker._leer_saldo_archivo("CUENTA_P1", 100001)
        if saldo == 5000.0:
            print("✓ Saldo leído correctamente: 5000.0")
        else:
            print(f"✗ Error leyendo saldo, esperado: 5000.0, obtenido: {saldo}")
            return False

        # Probar actualización de saldo único
        print("Probando actualización de saldo único...")
        if worker._actualizar_saldo_unica_cuenta("CUENTA_P1", 100001, 4500.0, "TEST"):
            print("✓ Saldo actualizado correctamente")
            # Verificar actualización
            nuevo_saldo = worker._leer_saldo_archivo("CUENTA_P1", 100001)
            if nuevo_saldo == 4500.0:
                print("✓ Verificación de actualización exitosa")
            else:
                print(
                    f"✗ Error en verificación, esperado: 4500.0, obtenido: {nuevo_saldo}"
                )
                return False
        else:
            print("✗ Error actualizando saldo")
            return False

        # Probar actualización de dos saldos (transferencia)
        print("Probando transferencia entre cuentas...")
        if worker._actualizar_saldos_archivo(
            "CUENTA_P1", 100002, 2500.0, 100003, 2000.0
        ):
            print("✓ Transferencia ejecutada correctamente")
            # Verificar ambos saldos
            saldo_origen = worker._leer_saldo_archivo("CUENTA_P1", 100002)
            saldo_destino = worker._leer_saldo_archivo("CUENTA_P1", 100003)
            if saldo_origen == 2500.0 and saldo_destino == 2000.0:
                print("✓ Verificación de transferencia exitosa")
            else:
                print(f"✗ Error en verificación de transferencia")
                print(f"  Origen esperado: 2500.0, obtenido: {saldo_origen}")
                print(f"  Destino esperado: 2000.0, obtenido: {saldo_destino}")
                return False
        else:
            print("✗ Error ejecutando transferencia")
            return False

        print("✓ Todas las pruebas de archivo pasaron")
        return True

    finally:
        # Limpiar directorio de prueba
        shutil.rmtree(test_dir)
        print(f"Directorio de prueba eliminado: {test_dir}")


def test_request_processing():
    """Prueba el procesamiento de solicitudes"""
    print("\n=== Test de Procesamiento de Solicitudes ===")

    # Crear directorio temporal
    test_dir = tempfile.mkdtemp(prefix="test_worker_req_")

    try:
        # Crear worker de prueba
        worker = NodoTrabajadorPython("test_worker_req", "localhost", 12346, 12352)
        worker.directorio_base_datos = test_dir
        worker.particiones_asignadas = ["CUENTA_P1"]

        # Crear datos de prueba
        test_data = {
            "CUENTA_P1": [
                {
                    "idCuenta": 100001,
                    "idCliente": 1,
                    "saldo": 5000.0,
                    "tipoCuenta": "Corriente",
                },
                {
                    "idCuenta": 100002,
                    "idCliente": 2,
                    "saldo": 3000.0,
                    "tipoCuenta": "Ahorros",
                },
            ]
        }
        worker._guardar_datos_particiones(test_data)

        # Test 1: Consultar saldo
        print("Probando consulta de saldo...")
        solicitud_saldo = {
            "tipoOperacion": TipoOperacion.CONSULTAR_SALDO,
            "parametros": {"ID_CUENTA": 100001, "ID_PARTICION": "CUENTA_P1"},
        }

        respuesta = worker.procesar_solicitud(solicitud_saldo)
        if (
            respuesta["estado"] == EstadoOperacion.EXITO
            and respuesta["datos"] == 5000.0
        ):
            print("✓ Consulta de saldo exitosa")
        else:
            print(f"✗ Error en consulta de saldo: {respuesta}")
            return False

        # Test 2: Transferencia exitosa
        print("Probando transferencia exitosa...")
        solicitud_transferencia = {
            "tipoOperacion": TipoOperacion.TRANSFERIR_FONDOS,
            "parametros": {
                "ID_CUENTA_ORIGEN": 100001,
                "ID_CUENTA_DESTINO": 100002,
                "MONTO": 1000.0,
                "ID_PARTICION": "CUENTA_P1",
                "ID_TRANSACCION_GLOBAL": 1,
            },
        }

        respuesta = worker.procesar_solicitud(solicitud_transferencia)
        if respuesta["estado"] == EstadoOperacion.EXITO:
            print("✓ Transferencia exitosa")
            datos = respuesta["datos"]
            if (
                datos["nuevoSaldoOrigen"] == 4000.0
                and datos["nuevoSaldoDestino"] == 4000.0
            ):
                print("✓ Saldos actualizados correctamente")
            else:
                print(f"✗ Error en saldos: {datos}")
                return False
        else:
            print(f"✗ Error en transferencia: {respuesta}")
            return False

        # Test 3: Transferencia con saldo insuficiente
        print("Probando transferencia con saldo insuficiente...")
        solicitud_insuficiente = {
            "tipoOperacion": TipoOperacion.TRANSFERIR_FONDOS,
            "parametros": {
                "ID_CUENTA_ORIGEN": 100001,
                "ID_CUENTA_DESTINO": 100002,
                "MONTO": 10000.0,
                "ID_PARTICION": "CUENTA_P1",
                "ID_TRANSACCION_GLOBAL": 2,
            },
        }

        respuesta = worker.procesar_solicitud(solicitud_insuficiente)
        if respuesta["estado"] == EstadoOperacion.ERROR_SALDO_INSUFICIENTE:
            print("✓ Error de saldo insuficiente detectado correctamente")
        else:
            print(f"✗ Error inesperado en transferencia insuficiente: {respuesta}")
            return False

        # Test 4: Preparar débito
        print("Probando preparación de débito...")
        solicitud_debito = {
            "tipoOperacion": TipoOperacion.PREPARAR_DEBITO,
            "parametros": {
                "ID_CUENTA_ORIGEN": 100001,
                "MONTO": 500.0,
                "ID_PARTICION": "CUENTA_P1",
                "ID_TRANSACCION_GLOBAL": 3,
            },
        }

        respuesta = worker.procesar_solicitud(solicitud_debito)
        if respuesta["estado"] == EstadoOperacion.DEBITO_PREPARADO_OK:
            print("✓ Débito preparado correctamente")
        else:
            print(f"✗ Error preparando débito: {respuesta}")
            return False

        print("✓ Todas las pruebas de procesamiento pasaron")
        return True

    finally:
        # Limpiar directorio de prueba
        shutil.rmtree(test_dir)
        print(f"Directorio de prueba eliminado: {test_dir}")


def test_java_object_translator():
    """Prueba el traductor de objetos Java"""
    print("\n=== Test de JavaObjectTranslator ===")

    # Test mensaje de registro
    mensaje_registro = JavaObjectTranslator.create_mensaje_worker_registro(
        "test_py", 12350
    )
    expected_fields = [
        "tipo",
        "workerId",
        "listaParticiones",
        "puertoTareasWorker",
        "mensajeTexto",
    ]

    if all(field in mensaje_registro for field in expected_fields):
        print("✓ Mensaje de registro creado correctamente")
    else:
        print(f"✗ Error en mensaje de registro: {mensaje_registro}")
        return False

    # Test mensaje de datos recibidos
    mensaje_datos = JavaObjectTranslator.create_mensaje_datos_recibidos("test_py")
    if mensaje_datos["tipo"] == "DATOS_RECIBIDOS_POR_WORKER":
        print("✓ Mensaje de datos recibidos creado correctamente")
    else:
        print(f"✗ Error en mensaje de datos recibidos: {mensaje_datos}")
        return False

    # Test respuesta
    respuesta = JavaObjectTranslator.create_respuesta(
        EstadoOperacion.EXITO, "Test OK", {"test": True}
    )
    if respuesta["estado"] == EstadoOperacion.EXITO and respuesta["datos"]["test"]:
        print("✓ Respuesta creada correctamente")
    else:
        print(f"✗ Error en respuesta: {respuesta}")
        return False

    print("✓ Todas las pruebas de JavaObjectTranslator pasaron")
    return True


def main():
    """Ejecuta todas las pruebas"""
    print("Iniciando pruebas del Worker Node Python")
    print("=" * 50)

    tests = [test_java_object_translator, test_file_operations, test_request_processing]

    passed = 0
    failed = 0

    for test in tests:
        try:
            if test():
                passed += 1
            else:
                failed += 1
        except Exception as e:
            print(f"✗ Excepción en {test.__name__}: {e}")
            failed += 1

    print("\n" + "=" * 50)
    print(f"Resultados: {passed} pruebas pasaron, {failed} fallaron")

    if failed == 0:
        print("✓ ¡Todas las pruebas pasaron! El worker está listo.")
        return True
    else:
        print("✗ Algunas pruebas fallaron. Revisar implementación.")
        return False


if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)

#!/usr/bin/env python3
"""
Standalone test for Python Worker Node
Simulates a complete banking system environment without Java server
"""

import os
import threading
import time
import socket
import json
from nodo_trabajador import NodoTrabajadorPython, JavaObjectTranslator


class MockJavaServer:
    """Simulates Java central server for testing Python worker"""

    def __init__(self, port=12347):
        self.port = port
        self.running = False
        self.workers = {}

    def start(self):
        """Start mock server"""
        self.running = True
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("localhost", self.port))
        server_socket.listen(5)

        print(f"Mock Java Server iniciado en puerto {self.port}")

        while self.running:
            try:
                client_socket, address = server_socket.accept()
                print(f"Mock Server: Worker conectado desde {address}")
                threading.Thread(
                    target=self.handle_worker_registration, args=(client_socket,)
                ).start()
            except:
                break

        server_socket.close()

    def handle_worker_registration(self, client_socket):
        """Handle worker registration process"""
        try:
            # Receive registration message
            length_bytes = client_socket.recv(4)
            if len(length_bytes) != 4:
                return
            length = int.from_bytes(length_bytes, byteorder="big")

            data = b""
            while len(data) < length:
                chunk = client_socket.recv(length - len(data))
                if not chunk:
                    return
                data += chunk

            registration_msg = json.loads(data.decode("utf-8"))
            worker_id = registration_msg.get("workerId")
            worker_port = registration_msg.get("puertoTareasWorker")

            print(
                f"Mock Server: Registro recibido de {worker_id} en puerto {worker_port}"
            )

            # Send partition assignment
            partition_data = {
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
                ],
                "CLIENTE_P1": [
                    {
                        "idCliente": 1,
                        "nombre": "Juan Pérez",
                        "email": "juan@test.com",
                        "telefono": "123456789",
                    },
                    {
                        "idCliente": 2,
                        "nombre": "Ana García",
                        "email": "ana@test.com",
                        "telefono": "987654321",
                    },
                    {
                        "idCliente": 3,
                        "nombre": "Carlos López",
                        "email": "carlos@test.com",
                        "telefono": "555666777",
                    },
                ],
            }

            assignment_msg = {
                "tipo": "ASIGNACION_PARTICIONES_Y_DATOS",
                "listaParticiones": ["CUENTA_P1", "CLIENTE_P1"],
                "datosPorParticion": partition_data,
                "mensajeTexto": "Particiones asignadas por Mock Server",
            }

            self._send_json_message(client_socket, assignment_msg)
            print(f"Mock Server: Asignación enviada a {worker_id}")

            # Receive confirmation
            confirmation = self._receive_json_message(client_socket)
            if (
                confirmation
                and confirmation.get("tipo") == "DATOS_RECIBIDOS_POR_WORKER"
            ):
                print(f"Mock Server: Confirmación recibida de {worker_id}")

                # Send final confirmation
                final_msg = {
                    "tipo": "CONFIRMACION_REGISTRO_COMPLETO",
                    "workerId": worker_id,
                    "mensajeTexto": "Registro completo - Mock Server",
                }
                self._send_json_message(client_socket, final_msg)
                print(f"Mock Server: Registro completo para {worker_id}")

                # Store worker info
                self.workers[worker_id] = {
                    "port": worker_port,
                    "partitions": ["CUENTA_P1", "CLIENTE_P1"],
                }

        except Exception as e:
            print(f"Mock Server: Error handling registration: {e}")
        finally:
            client_socket.close()

    def _send_json_message(self, sock, message):
        """Send JSON message with length header"""
        json_data = json.dumps(message).encode("utf-8")
        length = len(json_data)
        sock.sendall(length.to_bytes(4, byteorder="big"))
        sock.sendall(json_data)

    def _receive_json_message(self, sock):
        """Receive JSON message with length header"""
        try:
            length_bytes = sock.recv(4)
            if len(length_bytes) != 4:
                return None
            length = int.from_bytes(length_bytes, byteorder="big")

            data = b""
            while len(data) < length:
                chunk = sock.recv(length - len(data))
                if not chunk:
                    return None
                data += chunk

            return json.loads(data.decode("utf-8"))
        except:
            return None

    def stop(self):
        """Stop mock server"""
        self.running = False


class MockClient:
    """Simulates client requests to test worker functionality"""

    def __init__(self, worker_port=12351):
        self.worker_port = worker_port

    def send_request(self, request):
        """Send request to worker and get response"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect(("localhost", self.worker_port))

                # Send request
                json_data = json.dumps(request).encode("utf-8")
                length = len(json_data)
                sock.sendall(length.to_bytes(4, byteorder="big"))
                sock.sendall(json_data)

                # Receive response
                length_bytes = sock.recv(4)
                if len(length_bytes) != 4:
                    return None
                length = int.from_bytes(length_bytes, byteorder="big")

                data = b""
                while len(data) < length:
                    chunk = sock.recv(length - len(data))
                    if not chunk:
                        return None
                    data += chunk

                return json.loads(data.decode("utf-8"))
        except Exception as e:
            print(f"Mock Client: Error sending request: {e}")
            return None


def test_balance_query(client):
    """Test balance query"""
    print("\n=== Test: Consulta de Saldo ===")
    request = {
        "tipoOperacion": "CONSULTAR_SALDO",
        "parametros": {"ID_CUENTA": 100001, "ID_PARTICION": "CUENTA_P1"},
    }

    response = client.send_request(request)
    if response and response.get("estado") == "EXITO":
        print(f"✓ Saldo consultado: {response.get('datos')}")
        return True
    else:
        print(f"✗ Error en consulta: {response}")
        return False


def test_transfer(client):
    """Test fund transfer"""
    print("\n=== Test: Transferencia de Fondos ===")
    request = {
        "tipoOperacion": "TRANSFERIR_FONDOS",
        "parametros": {
            "ID_CUENTA_ORIGEN": 100001,
            "ID_CUENTA_DESTINO": 100002,
            "MONTO": 500.0,
            "ID_PARTICION": "CUENTA_P1",
            "ID_TRANSACCION_GLOBAL": 1,
        },
    }

    response = client.send_request(request)
    if response and response.get("estado") == "EXITO":
        data = response.get("datos", {})
        print(f"✓ Transferencia exitosa:")
        print(f"  - Nuevo saldo origen: {data.get('nuevoSaldoOrigen')}")
        print(f"  - Nuevo saldo destino: {data.get('nuevoSaldoDestino')}")
        return True
    else:
        print(f"✗ Error en transferencia: {response}")
        return False


def test_insufficient_funds(client):
    """Test insufficient funds scenario"""
    print("\n=== Test: Saldo Insuficiente ===")
    request = {
        "tipoOperacion": "TRANSFERIR_FONDOS",
        "parametros": {
            "ID_CUENTA_ORIGEN": 100002,
            "ID_CUENTA_DESTINO": 100003,
            "MONTO": 10000.0,  # Amount too high
            "ID_PARTICION": "CUENTA_P1",
            "ID_TRANSACCION_GLOBAL": 2,
        },
    }

    response = client.send_request(request)
    if response and response.get("estado") == "ERROR_SALDO_INSUFICIENTE":
        print(f"✓ Error de saldo insuficiente detectado correctamente")
        return True
    else:
        print(f"✗ Error inesperado: {response}")
        return False


def test_debit_preparation(client):
    """Test debit preparation for distributed transactions"""
    print("\n=== Test: Preparación de Débito ===")
    request = {
        "tipoOperacion": "PREPARAR_DEBITO",
        "parametros": {
            "ID_CUENTA_ORIGEN": 100001,
            "MONTO": 200.0,
            "ID_PARTICION": "CUENTA_P1",
            "ID_TRANSACCION_GLOBAL": 3,
        },
    }

    response = client.send_request(request)
    if response and response.get("estado") == "DEBITO_PREPARADO_OK":
        print(f"✓ Débito preparado correctamente")
        return True
    else:
        print(f"✗ Error en preparación de débito: {response}")
        return False


def main():
    """Main test function"""
    print("🐍 Standalone Test - Python Worker Node")
    print("=" * 50)

    # Start mock server
    mock_server = MockJavaServer()
    server_thread = threading.Thread(target=mock_server.start)
    server_thread.daemon = True
    server_thread.start()

    # Give server time to start
    time.sleep(1)

    # Start Python worker
    print("Iniciando Python Worker...")
    worker = NodoTrabajadorPython("worker_py_test", "localhost", 12347, 12351)
    worker_thread = threading.Thread(target=worker.iniciar)
    worker_thread.daemon = True
    worker_thread.start()

    # Give worker time to register
    time.sleep(2)

    # Test with mock client
    client = MockClient(12351)

    tests = [
        test_balance_query,
        test_transfer,
        test_insufficient_funds,
        test_debit_preparation,
    ]

    passed = 0
    total = len(tests)

    for test_func in tests:
        try:
            if test_func(client):
                passed += 1
            time.sleep(0.5)  # Small delay between tests
        except Exception as e:
            print(f"✗ Excepción en {test_func.__name__}: {e}")

    print("\n" + "=" * 50)
    print(f"Resultados: {passed}/{total} pruebas pasaron")

    if passed == total:
        print("✓ ¡Todas las pruebas de integración pasaron!")
        print("✓ El worker Python funciona correctamente en modo standalone")
    else:
        print("✗ Algunas pruebas fallaron")

    # Cleanup
    mock_server.stop()
    return passed == total


if __name__ == "__main__":
    success = main()
    print("\n🏁 Test finalizado")
    time.sleep(1)  # Give threads time to cleanup

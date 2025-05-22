#!/usr/bin/env python3
"""
Protocol Bridge: Translates between Python Worker (JSON) and Java Server (ObjectStream)
This allows the existing Python worker to communicate with Java server without changes
"""

import json
import socket
import threading
import logging
import struct
from typing import Dict, Any, Optional

try:
    import javaobj
except ImportError:
    print("Install javaobj-py3: pip install javaobj-py3")
    exit(1)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ProtocolBridge:
    """Bridge between JSON (Python) and ObjectStream (Java) protocols"""

    def __init__(self, java_server_host: str, java_server_port: int, bridge_port: int):
        self.java_server_host = java_server_host
        self.java_server_port = java_server_port
        self.bridge_port = bridge_port
        self.running = False

    def start(self):
        """Start the protocol bridge server"""
        self.running = True
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server_socket.bind(("localhost", self.bridge_port))
        server_socket.listen(5)

        logger.info(f"Protocol Bridge started on port {self.bridge_port}")
        logger.info(
            f"Forwarding to Java server at {self.java_server_host}:{self.java_server_port}"
        )

        while self.running:
            try:
                client_socket, address = server_socket.accept()
                logger.info(f"Python worker connected from {address}")
                threading.Thread(
                    target=self.handle_python_worker, args=(client_socket,), daemon=True
                ).start()
            except Exception as e:
                if self.running:
                    logger.error(f"Error accepting connection: {e}")

        server_socket.close()

    def handle_python_worker(self, python_socket: socket.socket):
        """Handle communication from Python worker"""
        java_socket = None
        try:
            # Connect to Java server
            java_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            java_socket.connect((self.java_server_host, self.java_server_port))
            logger.info("Connected to Java server")

            # Handle the complete registration process
            self.bridge_registration_process(python_socket, java_socket)

        except Exception as e:
            logger.error(f"Error in bridge: {e}")
        finally:
            python_socket.close()
            if java_socket:
                try:
                    java_socket.close()
                except:
                    pass

    def bridge_registration_process(
        self, python_socket: socket.socket, java_socket: socket.socket
    ):
        """Bridge the complete worker registration process"""
        try:
            # 1. Receive JSON registration from Python worker
            json_msg = self.receive_json_message(python_socket)
            if not json_msg:
                logger.error("Failed to receive registration from Python worker")
                return

            logger.info(f"Received JSON registration: {json_msg}")

            # 2. Convert to Java object and send to server
            java_obj = self.create_java_mensaje_worker(json_msg)
            self.send_java_object(java_socket, java_obj)
            logger.info("Sent Java registration to server")

            # 3. Receive Java response and convert to JSON
            java_response = self.receive_java_object(java_socket)
            if not java_response:
                logger.error("Failed to receive response from Java server")
                return

            logger.info(f"Received Java response: {java_response}")
            json_response = self.convert_java_to_json_response(java_response)
            self.send_json_message(python_socket, json_response)
            logger.info("Sent JSON response to Python worker")

            # 4. Handle confirmation from Python worker
            confirmation = self.receive_json_message(python_socket)
            if confirmation:
                logger.info(f"Received confirmation from Python: {confirmation}")
                java_confirm = self.create_java_mensaje_worker(confirmation)
                self.send_java_object(java_socket, java_confirm)
                logger.info("Sent Java confirmation to server")

                # 5. Final response from Java server
                final_response = self.receive_java_object(java_socket)
                if final_response:
                    logger.info(f"Received final Java response: {final_response}")
                    final_json = self.convert_java_to_json_response(final_response)
                    self.send_json_message(python_socket, final_json)
                    logger.info("Bridge registration process completed successfully!")

        except Exception as e:
            logger.error(f"Error in registration bridge: {e}")

    def receive_json_message(self, sock: socket.socket) -> Optional[Dict]:
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
        except Exception as e:
            logger.error(f"Error receiving JSON message: {e}")
            return None

    def send_json_message(self, sock: socket.socket, message: Dict):
        """Send JSON message with length header"""
        json_data = json.dumps(message).encode("utf-8")
        length = len(json_data)
        sock.sendall(length.to_bytes(4, byteorder="big"))
        sock.sendall(json_data)

    def send_java_object(self, sock: socket.socket, obj: Dict):
        """Send object as Java serialized data"""
        # Convert Python dict to Java object using javaobj
        java_obj = javaobj.JavaObject()
        java_obj.classdesc = javaobj.JavaClass()
        java_obj.classdesc.name = obj.get("className", "commons.MensajeWorker")

        # Set fields
        for key, value in obj.items():
            if key != "className":
                setattr(java_obj, key, value)

        # Serialize and send
        java_bytes = javaobj.dumps(java_obj)
        sock.sendall(java_bytes)

    def receive_java_object(self, sock: socket.socket) -> Optional[Dict]:
        """Receive Java serialized object"""
        try:
            # Read enough bytes for a typical Java object
            # This is a simplified approach - in production you'd need proper stream handling
            data = sock.recv(8192)
            if not data:
                return None

            java_obj = javaobj.loads(data)

            # Convert Java object to dict
            result = {}
            if hasattr(java_obj, "__dict__"):
                for key, value in java_obj.__dict__.items():
                    if not key.startswith("_"):
                        result[key] = value

            return result
        except Exception as e:
            logger.error(f"Error receiving Java object: {e}")
            return None

    def create_java_mensaje_worker(self, json_obj: Dict) -> Dict:
        """Convert JSON message to Java MensajeWorker format"""
        return {
            "className": "commons.MensajeWorker",
            "tipo": json_obj.get("tipo"),
            "workerId": json_obj.get("workerId"),
            "listaParticiones": json_obj.get("listaParticiones", []),
            "puertoTareasWorker": json_obj.get("puertoTareasWorker"),
            "mensajeTexto": json_obj.get("mensajeTexto"),
            "datosPorParticion": json_obj.get("datosPorParticion", {}),
        }

    def convert_java_to_json_response(self, java_obj: Dict) -> Dict:
        """Convert Java object to JSON format for Python worker"""
        return {
            "tipo": java_obj.get("tipo"),
            "listaParticiones": java_obj.get("listaParticiones", []),
            "datosPorParticion": java_obj.get("datosPorParticion", {}),
            "mensajeTexto": java_obj.get("mensajeTexto"),
            "workerId": java_obj.get("workerId"),
        }


def main():
    """Run the protocol bridge"""
    import sys

    if len(sys.argv) != 4:
        print("Usage: python protocol_bridge.py <java_host> <java_port> <bridge_port>")
        print("Example: python protocol_bridge.py localhost 12346 12347")
        sys.exit(1)

    java_host = sys.argv[1]
    java_port = int(sys.argv[2])
    bridge_port = int(sys.argv[3])

    bridge = ProtocolBridge(java_host, java_port, bridge_port)

    try:
        bridge.start()
    except KeyboardInterrupt:
        logger.info("Bridge shutting down...")
        bridge.running = False


if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
NodoTrabajador Python - Compatible con Sistema Bancario Distribuido Java
"""

import socket
import json
import pickle
import threading
import os
import csv
import sys
import time
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from datetime import datetime
import logging

# Configurar logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


@dataclass
class Cuenta:
    id_cuenta: int
    id_cliente: int
    saldo: float
    tipo_cuenta: str


@dataclass
class Cliente:
    id_cliente: int
    nombre: str
    email: str
    telefono: str


class EstadoOperacion:
    EXITO = "EXITO"
    ERROR_SALDO_INSUFICIENTE = "ERROR_SALDO_INSUFICIENTE"
    ERROR_CUENTA_ORIGEN_NO_EXISTE = "ERROR_CUENTA_ORIGEN_NO_EXISTE"
    ERROR_CUENTA_DESTINO_NO_EXISTE = "ERROR_CUENTA_DESTINO_NO_EXISTE"
    ERROR_GENERAL_SERVIDOR = "ERROR_GENERAL_SERVIDOR"
    ERROR_COMUNICACION = "ERROR_COMUNICACION"
    DEBITO_PREPARADO_OK = "DEBITO_PREPARADO_OK"
    DEBITO_CONFIRMADO_OK = "DEBITO_CONFIRMADO_OK"
    DEBITO_REVERTIDO_OK = "DEBITO_REVERTIDO_OK"
    CREDITO_APLICADO_OK = "CREDITO_APLICADO_OK"
    REPLICA_ACTUALIZADA_OK = "REPLICA_ACTUALIZADA_OK"


class TipoOperacion:
    CONSULTAR_SALDO = "CONSULTAR_SALDO"
    TRANSFERIR_FONDOS = "TRANSFERIR_FONDOS"
    PREPARAR_DEBITO = "PREPARAR_DEBITO"
    CONFIRMAR_DEBITO = "CONFIRMAR_DEBITO"
    REVERTIR_DEBITO = "REVERTIR_DEBITO"
    APLICAR_CREDITO = "APLICAR_CREDITO"
    ACTUALIZAR_SALDO_REPLICA = "ACTUALIZAR_SALDO_REPLICA"
    CALCULAR_SALDO_PARTICION = "CALCULAR_SALDO_PARTICION"


class JavaObjectTranslator:
    """Traductor para comunicación con objetos Java"""

    @staticmethod
    def create_mensaje_worker_registro(worker_id: str, puerto_tareas: int):
        """Crea mensaje de registro compatible con Java"""
        return {
            "className": "commons.MensajeWorker",
            "tipo": "REGISTRO",
            "workerId": worker_id,
            "listaParticiones": [],
            "puertoTareasWorker": puerto_tareas,
            "mensajeTexto": "Solicitud de registro",
        }

    @staticmethod
    def create_mensaje_datos_recibidos(worker_id: str):
        """Mensaje confirmando recepción de datos"""
        return {
            "className": "commons.MensajeWorker",
            "tipo": "DATOS_RECIBIDOS_POR_WORKER",
            "workerId": worker_id,
            "mensajeTexto": "Datos de partición recibidos y guardados.",
        }

    @staticmethod
    def create_respuesta(estado: str, mensaje: str, datos: Any = None):
        """Crea respuesta compatible con Java"""
        return {
            "className": "commons.Respuesta",
            "estado": estado,
            "mensaje": mensaje,
            "datos": datos,
        }

    @staticmethod
    def parse_java_object(obj_dict: Dict) -> Dict:
        """Parsea objeto recibido de Java"""
        return obj_dict


class NodoTrabajadorPython:
    def __init__(
        self,
        worker_id: str,
        host_servidor_central: str,
        puerto_servidor_central: int,
        puerto_escucha_tareas: int,
    ):
        self.worker_id = worker_id
        self.host_servidor_central = host_servidor_central
        self.puerto_servidor_central = puerto_servidor_central
        self.puerto_escucha_tareas = puerto_escucha_tareas
        self.particiones_asignadas = []
        self.directorio_base_datos = f"data_{worker_id}"
        self.archivo_log_transacciones = os.path.join(
            self.directorio_base_datos, "transacciones_locales.log"
        )
        self.registrado_y_datos_cargados = False
        self.executor = ThreadPoolExecutor(max_workers=10)

        # Crear directorio de datos
        os.makedirs(self.directorio_base_datos, exist_ok=True)

        logger.info(
            f"Worker [{worker_id}]: Inicializado con directorio {self.directorio_base_datos}"
        )

    def iniciar(self):
        """Inicia el proceso de registro y escucha de tareas"""
        if self.registrar_y_recibir_datos():
            logger.info(
                f"Worker [{self.worker_id}]: Registrado exitosamente. Iniciando escucha de tareas en puerto {self.puerto_escucha_tareas}"
            )
            self.registrado_y_datos_cargados = True
            self.escuchar_tareas()
        else:
            logger.error(f"Worker [{self.worker_id}]: Fallo en registro. Abortando.")

    def registrar_y_recibir_datos(self) -> bool:
        """Registra el worker con el servidor central y recibe datos de partición"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                # Optimizar socket para mejor rendimiento
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
                sock.settimeout(60.0)  # 60 segundos timeout
                sock.connect((self.host_servidor_central, self.puerto_servidor_central))
                logger.info(
                    f"Worker [{self.worker_id}]: Conectado a servidor central {self.host_servidor_central}:{self.puerto_servidor_central}"
                )

                # Enviar mensaje de registro
                msg_registro = JavaObjectTranslator.create_mensaje_worker_registro(
                    self.worker_id, self.puerto_escucha_tareas
                )
                self._enviar_mensaje_json(sock, msg_registro)
                logger.info(f"Worker [{self.worker_id}]: Mensaje de REGISTRO enviado")

                # Recibir asignación de particiones
                respuesta = self._recibir_mensaje_json(sock)
                if (
                    not respuesta
                    or respuesta.get("tipo") != "ASIGNACION_PARTICIONES_Y_DATOS"
                ):
                    logger.error(
                        f"Worker [{self.worker_id}]: Respuesta inesperada del servidor"
                    )
                    return False

                logger.info(
                    f"Worker [{self.worker_id}]: Recibida asignación de particiones: {respuesta.get('listaParticiones', [])}"
                )

                # Guardar datos de particiones
                if self._guardar_datos_particiones(
                    respuesta.get("datosPorParticion", {})
                ):
                    self.particiones_asignadas = respuesta.get("listaParticiones", [])

                    # Confirmar recepción de datos
                    msg_confirmacion = (
                        JavaObjectTranslator.create_mensaje_datos_recibidos(
                            self.worker_id
                        )
                    )
                    self._enviar_mensaje_json(sock, msg_confirmacion)
                    logger.info(
                        f"Worker [{self.worker_id}]: Confirmación de datos recibidos enviada"
                    )

                    # Recibir confirmación final
                    confirmacion_final = self._recibir_mensaje_json(sock)
                    if (
                        confirmacion_final
                        and confirmacion_final.get("tipo")
                        == "CONFIRMACION_REGISTRO_COMPLETO"
                    ):
                        logger.info(
                            f"Worker [{self.worker_id}]: Registro completado: {confirmacion_final.get('mensajeTexto', '')}"
                        )
                        return True
                    else:
                        logger.error(
                            f"Worker [{self.worker_id}]: No se recibió confirmación final de registro"
                        )
                else:
                    logger.error(
                        f"Worker [{self.worker_id}]: Error guardando datos de partición"
                    )

        except Exception as e:
            logger.error(f"Worker [{self.worker_id}]: Error durante registro: {e}")

        return False

    def _enviar_mensaje_json(self, sock: socket.socket, mensaje: Dict):
        """Envía mensaje en formato JSON compatible con Java"""
        json_data = json.dumps(mensaje).encode("utf-8")
        length = len(json_data)
        sock.sendall(length.to_bytes(4, byteorder="big"))
        sock.sendall(json_data)

    def _recibir_mensaje_json(self, sock: socket.socket) -> Optional[Dict]:
        """Recibe mensaje en formato JSON del servidor Java"""
        try:
            logger.info(
                f"Worker [{self.worker_id}]: Esperando respuesta del servidor..."
            )
            # Leer longitud del mensaje
            length_bytes = sock.recv(4)
            if len(length_bytes) != 4:
                logger.error(
                    f"Worker [{self.worker_id}]: No se pudo leer header de longitud, recibidos {len(length_bytes)} bytes"
                )
                return None
            length = int.from_bytes(length_bytes, byteorder="big")
            logger.info(
                f"Worker [{self.worker_id}]: Leyendo mensaje de longitud {length}"
            )

            # Leer mensaje completo en chunks más grandes para eficiencia
            data = b""
            chunk_size = min(
                65536, length
            )  # 64KB chunks o el tamaño del mensaje si es menor
            while len(data) < length:
                remaining = length - len(data)
                to_read = min(chunk_size, remaining)
                chunk = sock.recv(to_read)
                if not chunk:
                    logger.error(
                        f"Worker [{self.worker_id}]: Conexión cerrada durante lectura de mensaje"
                    )
                    return None
                data += chunk

                # Log progreso para mensajes grandes
                if length > 100000:  # Solo para mensajes > 100KB
                    progress = (len(data) / length) * 100
                    if (
                        len(data) % 100000 == 0 or len(data) == length
                    ):  # Log cada 100KB o al final
                        logger.info(
                            f"Worker [{self.worker_id}]: Progreso lectura: {progress:.1f}% ({len(data)}/{length} bytes)"
                        )

            message = json.loads(data.decode("utf-8"))
            logger.info(f"Worker [{self.worker_id}]: Mensaje recibido: {message}")
            return message
        except Exception as e:
            logger.error(f"Worker [{self.worker_id}]: Error recibiendo mensaje: {e}")
            return None

    def _guardar_datos_particiones(self, datos_por_particion: Dict) -> bool:
        """Guarda los datos de partición recibidos en archivos locales"""
        try:
            for id_particion, lista_datos in datos_por_particion.items():
                nombre_archivo = os.path.join(
                    self.directorio_base_datos, f"{id_particion}.txt"
                )

                with open(nombre_archivo, "w", encoding="utf-8") as f:
                    if id_particion.startswith("CUENTA_P"):
                        f.write("ID_CUENTA|ID_CLIENTE|SALDO|TIPO_CUENTA\n")
                        for dato in lista_datos:
                            if isinstance(dato, dict) and "idCuenta" in dato:
                                f.write(
                                    f"{dato['idCuenta']}|{dato['idCliente']}|{dato['saldo']:.2f}|{dato['tipoCuenta']}\n"
                                )
                    elif id_particion.startswith("CLIENTE_P"):
                        f.write("ID_CLIENTE|NOMBRE|EMAIL|TELEFONO\n")
                        for dato in lista_datos:
                            if isinstance(dato, dict) and "idCliente" in dato:
                                f.write(
                                    f"{dato['idCliente']}|{dato['nombre']}|{dato['email']}|{dato['telefono']}\n"
                                )

                logger.info(
                    f"Worker [{self.worker_id}]: Datos para partición {id_particion} guardados en {nombre_archivo}"
                )

            return True
        except Exception as e:
            logger.error(
                f"Worker [{self.worker_id}]: Error guardando datos de particiones: {e}"
            )
            return False

    def escuchar_tareas(self):
        """Escucha conexiones para tareas del servidor central"""
        try:
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server_socket:
                server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                server_socket.bind(("localhost", self.puerto_escucha_tareas))
                server_socket.listen(10)
                logger.info(
                    f"Worker [{self.worker_id}]: Escuchando tareas en puerto {self.puerto_escucha_tareas}"
                )

                while self.registrado_y_datos_cargados:
                    try:
                        client_socket, address = server_socket.accept()
                        logger.info(
                            f"Worker [{self.worker_id}]: Tarea recibida de {address}"
                        )
                        self.executor.submit(self._manejar_tarea, client_socket)
                    except Exception as e:
                        if self.registrado_y_datos_cargados:
                            logger.error(
                                f"Worker [{self.worker_id}]: Error aceptando conexión: {e}"
                            )

        except Exception as e:
            logger.error(
                f"Worker [{self.worker_id}]: Error crítico en escucha de tareas: {e}"
            )

    def _manejar_tarea(self, client_socket: socket.socket):
        """Maneja una tarea específica de un cliente"""
        try:
            with client_socket:
                solicitud = self._recibir_mensaje_json(client_socket)
                if solicitud:
                    logger.info(
                        f"Worker [{self.worker_id}]: Procesando {solicitud.get('tipoOperacion', 'UNKNOWN')}"
                    )
                    respuesta = self.procesar_solicitud(solicitud)
                    self._enviar_mensaje_json(client_socket, respuesta)
                    logger.info(
                        f"Worker [{self.worker_id}]: Respuesta enviada: {respuesta.get('estado', 'UNKNOWN')}"
                    )
                else:
                    logger.error(
                        f"Worker [{self.worker_id}]: Solicitud inválida recibida"
                    )
        except Exception as e:
            logger.error(f"Worker [{self.worker_id}]: Error manejando tarea: {e}")

    def procesar_solicitud(self, solicitud: Dict) -> Dict:
        """Procesa una solicitud y retorna la respuesta"""
        tipo_operacion = solicitud.get("tipoOperacion")
        parametros = solicitud.get("parametros", {})
        id_transaccion_global = parametros.get("ID_TRANSACCION_GLOBAL", -1)
        id_particion = parametros.get("ID_PARTICION")

        logger.info(
            f"Worker [{self.worker_id}]: Procesando {tipo_operacion} con params: {parametros}"
        )

        # Verificar si el worker maneja la partición
        if id_particion and id_particion not in self.particiones_asignadas:
            error_msg = f"Worker no maneja la partición {id_particion} para operación {tipo_operacion}"
            logger.error(f"Worker [{self.worker_id}]: {error_msg}")
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR, error_msg
            )

        if tipo_operacion == TipoOperacion.CONSULTAR_SALDO:
            return self._consultar_saldo(parametros, id_particion)
        elif tipo_operacion == TipoOperacion.TRANSFERIR_FONDOS:
            return self._transferir_fondos(
                parametros, id_particion, id_transaccion_global
            )
        elif tipo_operacion == TipoOperacion.PREPARAR_DEBITO:
            return self._preparar_debito(
                parametros, id_particion, id_transaccion_global
            )
        elif tipo_operacion == TipoOperacion.APLICAR_CREDITO:
            return self._aplicar_credito(
                parametros, id_particion, id_transaccion_global
            )
        elif tipo_operacion == TipoOperacion.CONFIRMAR_DEBITO:
            return self._confirmar_debito(
                parametros, id_particion, id_transaccion_global
            )
        elif tipo_operacion == TipoOperacion.REVERTIR_DEBITO:
            return self._revertir_debito(
                parametros, id_particion, id_transaccion_global
            )
        elif tipo_operacion == TipoOperacion.ACTUALIZAR_SALDO_REPLICA:
            return self._actualizar_saldo_replica(
                parametros, id_particion, id_transaccion_global
            )
        elif tipo_operacion == TipoOperacion.CALCULAR_SALDO_PARTICION:
            return self._calcular_saldo_particion(parametros, id_particion)
        else:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                f"Operación no soportada: {tipo_operacion}",
            )

    def _consultar_saldo(self, parametros: Dict, id_particion: str) -> Dict:
        """Consulta el saldo de una cuenta"""
        id_cuenta = parametros.get("ID_CUENTA")
        if not id_cuenta or not id_particion:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                "Parámetros incompletos CONSULTAR_SALDO",
            )

        saldo = self._leer_saldo_archivo(id_particion, id_cuenta)
        if saldo is not None:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.EXITO, f"Saldo: {saldo}", saldo
            )
        else:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_CUENTA_ORIGEN_NO_EXISTE,
                f"Cuenta {id_cuenta} no existe en partición {id_particion}",
            )

    def _transferir_fondos(
        self, parametros: Dict, id_particion: str, id_transaccion_global: int
    ) -> Dict:
        """Realiza transferencia de fondos dentro de la misma partición"""
        id_cuenta_origen = parametros.get("ID_CUENTA_ORIGEN")
        id_cuenta_destino = parametros.get("ID_CUENTA_DESTINO")
        monto = parametros.get("MONTO")

        if not all([id_cuenta_origen, id_cuenta_destino, monto, id_particion]):
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                "Parámetros incompletos TRANSFERIR_FONDOS",
            )

        # Verificar saldo origen
        saldo_origen = self._leer_saldo_archivo(id_particion, id_cuenta_origen)
        if saldo_origen is None:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_CUENTA_ORIGEN_NO_EXISTE,
                f"Cuenta origen {id_cuenta_origen} no existe",
            )

        if saldo_origen < monto:
            self._registrar_transaccion_local(
                id_transaccion_global,
                id_cuenta_origen,
                id_cuenta_destino,
                monto,
                f"RECHAZADA_SALDO_INSUF_W{self.worker_id}",
            )
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_SALDO_INSUFICIENTE,
                f"Saldo insuficiente en cuenta {id_cuenta_origen}",
                saldo_origen,
            )

        # Verificar cuenta destino
        saldo_destino = self._leer_saldo_archivo(id_particion, id_cuenta_destino)
        if saldo_destino is None:
            self._registrar_transaccion_local(
                id_transaccion_global,
                id_cuenta_origen,
                id_cuenta_destino,
                monto,
                f"FALLIDA_DESTINO_NO_EXISTE_W{self.worker_id}",
            )
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_CUENTA_DESTINO_NO_EXISTE,
                f"Cuenta destino {id_cuenta_destino} no existe",
            )

        # Realizar transferencia
        nuevo_saldo_origen = saldo_origen - monto
        nuevo_saldo_destino = saldo_destino + monto

        if self._actualizar_saldos_archivo(
            id_particion,
            id_cuenta_origen,
            nuevo_saldo_origen,
            id_cuenta_destino,
            nuevo_saldo_destino,
        ):
            self._registrar_transaccion_local(
                id_transaccion_global,
                id_cuenta_origen,
                id_cuenta_destino,
                monto,
                f"EXITO_INTRA_PARTICION_W{self.worker_id}",
            )
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.EXITO,
                f"Transferencia intra-partición OK por {self.worker_id}",
                {
                    "nuevoSaldoOrigen": nuevo_saldo_origen,
                    "nuevoSaldoDestino": nuevo_saldo_destino,
                    "workerIdProcesador": self.worker_id,
                },
            )
        else:
            self._registrar_transaccion_local(
                id_transaccion_global,
                id_cuenta_origen,
                id_cuenta_destino,
                monto,
                f"FALLIDA_ESCRITURA_W{self.worker_id}",
            )
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                f"Error de escritura en {id_particion}",
            )

    def _preparar_debito(
        self, parametros: Dict, id_particion: str, id_transaccion_global: int
    ) -> Dict:
        """Prepara un débito verificando saldo suficiente"""
        id_cuenta_origen = parametros.get("ID_CUENTA_ORIGEN")
        monto = parametros.get("MONTO")

        if not all([id_cuenta_origen, monto, id_particion]):
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                "Parámetros incompletos PREPARAR_DEBITO",
            )

        saldo_origen = self._leer_saldo_archivo(id_particion, id_cuenta_origen)
        if saldo_origen is None:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_CUENTA_ORIGEN_NO_EXISTE,
                f"Cuenta origen {id_cuenta_origen} no existe",
            )

        if saldo_origen < monto:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_SALDO_INSUFICIENTE,
                f"Saldo insuficiente en cuenta {id_cuenta_origen}",
                saldo_origen,
            )

        self._registrar_transaccion_local(
            id_transaccion_global,
            id_cuenta_origen,
            -1,
            monto,
            f"PREPARAR_DEBITO_OK_W{self.worker_id}",
        )
        return JavaObjectTranslator.create_respuesta(
            EstadoOperacion.DEBITO_PREPARADO_OK,
            f"Débito preparado por {self.worker_id}",
            {"saldoActualOrigen": saldo_origen, "workerIdProcesador": self.worker_id},
        )

    def _aplicar_credito(
        self, parametros: Dict, id_particion: str, id_transaccion_global: int
    ) -> Dict:
        """Aplica un crédito a una cuenta"""
        id_cuenta_destino = parametros.get("ID_CUENTA_DESTINO")
        monto = parametros.get("MONTO")

        if not all([id_cuenta_destino, monto, id_particion]):
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                "Parámetros incompletos APLICAR_CREDITO",
            )

        saldo_destino = self._leer_saldo_archivo(id_particion, id_cuenta_destino)
        if saldo_destino is None:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_CUENTA_DESTINO_NO_EXISTE,
                f"Cuenta destino {id_cuenta_destino} no existe",
            )

        nuevo_saldo = saldo_destino + monto
        if self._actualizar_saldo_unica_cuenta(
            id_particion, id_cuenta_destino, nuevo_saldo, "APLICAR_CREDITO"
        ):
            self._registrar_transaccion_local(
                id_transaccion_global,
                -1,
                id_cuenta_destino,
                monto,
                f"APLICAR_CREDITO_OK_W{self.worker_id}",
            )
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.CREDITO_APLICADO_OK,
                f"Crédito aplicado por {self.worker_id}",
                {
                    "nuevoSaldoDestino": nuevo_saldo,
                    "workerIdProcesador": self.worker_id,
                },
            )
        else:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                "Error de escritura APLICAR_CREDITO",
            )

    def _confirmar_debito(
        self, parametros: Dict, id_particion: str, id_transaccion_global: int
    ) -> Dict:
        """Confirma un débito previamente preparado"""
        id_cuenta_origen = parametros.get("ID_CUENTA_ORIGEN")
        monto = parametros.get("MONTO")

        if not all([id_cuenta_origen, monto, id_particion]):
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                "Parámetros incompletos CONFIRMAR_DEBITO",
            )

        saldo_origen = self._leer_saldo_archivo(id_particion, id_cuenta_origen)
        if saldo_origen is None:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_CUENTA_ORIGEN_NO_EXISTE,
                f"Cuenta origen {id_cuenta_origen} no existe (CONFIRMAR_DEBITO)",
            )

        nuevo_saldo = saldo_origen - monto
        if self._actualizar_saldo_unica_cuenta(
            id_particion, id_cuenta_origen, nuevo_saldo, "CONFIRMAR_DEBITO"
        ):
            self._registrar_transaccion_local(
                id_transaccion_global,
                id_cuenta_origen,
                -1,
                monto,
                f"CONFIRMAR_DEBITO_OK_W{self.worker_id}",
            )
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.DEBITO_CONFIRMADO_OK,
                f"Débito confirmado por {self.worker_id}",
                {"nuevoSaldoOrigen": nuevo_saldo, "workerIdProcesador": self.worker_id},
            )
        else:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                "Error de escritura CONFIRMAR_DEBITO",
            )

    def _revertir_debito(
        self, parametros: Dict, id_particion: str, id_transaccion_global: int
    ) -> Dict:
        """Revierte un débito previamente preparado"""
        id_cuenta_origen = parametros.get("ID_CUENTA_ORIGEN")
        monto = parametros.get("MONTO")

        if not all([id_cuenta_origen, monto, id_particion]):
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                "Parámetros incompletos REVERTIR_DEBITO",
            )

        saldo_origen = self._leer_saldo_archivo(id_particion, id_cuenta_origen)
        if saldo_origen is None:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_CUENTA_ORIGEN_NO_EXISTE,
                f"Cuenta origen {id_cuenta_origen} no existe (REVERTIR_DEBITO)",
            )

        nuevo_saldo = saldo_origen + monto  # Sumar de vuelta
        if self._actualizar_saldo_unica_cuenta(
            id_particion, id_cuenta_origen, nuevo_saldo, "REVERTIR_DEBITO"
        ):
            self._registrar_transaccion_local(
                id_transaccion_global,
                id_cuenta_origen,
                -1,
                monto,
                f"REVERTIR_DEBITO_OK_W{self.worker_id}",
            )
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.DEBITO_REVERTIDO_OK,
                f"Débito revertido por {self.worker_id}",
                {"nuevoSaldoOrigen": nuevo_saldo, "workerIdProcesador": self.worker_id},
            )
        else:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                "Error de escritura REVERTIR_DEBITO",
            )

    def _actualizar_saldo_replica(
        self, parametros: Dict, id_particion: str, id_transaccion_global: int
    ) -> Dict:
        """Actualiza saldo en réplica"""
        id_cuenta_replica = parametros.get("ID_CUENTA")
        nuevo_saldo_replica = parametros.get("NUEVO_SALDO")

        if not all(
            [
                id_cuenta_replica is not None,
                nuevo_saldo_replica is not None,
                id_particion,
            ]
        ):
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                "Parámetros incompletos ACTUALIZAR_SALDO_REPLICA",
            )

        if self._actualizar_saldo_unica_cuenta(
            id_particion,
            id_cuenta_replica,
            nuevo_saldo_replica,
            "ACTUALIZAR_SALDO_REPLICA",
        ):
            logger.info(
                f"Worker [{self.worker_id}] [Tx:{id_transaccion_global}]: Réplica actualizada para cuenta {id_cuenta_replica} en partición {id_particion}"
            )
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.REPLICA_ACTUALIZADA_OK,
                f"Réplica actualizada por {self.worker_id}",
            )
        else:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                "Error de escritura ACTUALIZAR_SALDO_REPLICA",
            )

    def _calcular_saldo_particion(self, parametros: Dict, id_particion: str) -> Dict:
        """Calcula el saldo total de una partición"""
        if not id_particion:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                "ID_PARTICION no proporcionado para CALCULAR_SALDO_PARTICION",
            )

        if id_particion not in self.particiones_asignadas:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                f"Worker no maneja la partición {id_particion} para arqueo",
            )

        suma_saldos = 0.0
        nombre_archivo = os.path.join(self.directorio_base_datos, f"{id_particion}.txt")

        try:
            with open(nombre_archivo, "r", encoding="utf-8") as f:
                next(f)  # Saltar cabecera
                for linea in f:
                    partes = linea.strip().split("|")
                    if len(partes) >= 3:
                        try:
                            saldo_cuenta = float(partes[2].replace(",", "."))
                            suma_saldos += saldo_cuenta
                        except ValueError as e:
                            logger.error(
                                f"Worker [{self.worker_id}]: Error parseando saldo en línea (arqueo): {linea.strip()} - {e}"
                            )

            logger.info(
                f"Worker [{self.worker_id}]: Suma de saldos para partición {id_particion} es: {suma_saldos}"
            )
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.EXITO,
                f"Suma de saldos de partición {id_particion} calculada por worker {self.worker_id}",
                suma_saldos,
            )
        except FileNotFoundError:
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                f"Archivo de partición no encontrado: {nombre_archivo}",
            )
        except Exception as e:
            logger.error(
                f"Worker [{self.worker_id}]: Error calculando saldo de partición: {e}"
            )
            return JavaObjectTranslator.create_respuesta(
                EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                "Error al calcular saldo de partición",
            )

    def _leer_saldo_archivo(
        self, id_particion: str, id_cuenta_buscada: int
    ) -> Optional[float]:
        """Lee el saldo de una cuenta específica del archivo de partición"""
        nombre_archivo = os.path.join(self.directorio_base_datos, f"{id_particion}.txt")

        try:
            with open(nombre_archivo, "r", encoding="utf-8") as f:
                next(f)  # Saltar cabecera
                for linea in f:
                    partes = linea.strip().split("|")
                    if len(partes) >= 3:
                        try:
                            id_cuenta_archivo = int(partes[0])
                            if id_cuenta_archivo == id_cuenta_buscada:
                                return float(partes[2].replace(",", "."))
                        except ValueError as e:
                            logger.error(
                                f"Worker [{self.worker_id}]: Error parseando línea en {nombre_archivo}: {linea.strip()} - {e}"
                            )
        except FileNotFoundError:
            logger.error(
                f"Worker [{self.worker_id}]: Archivo de partición no encontrado: {nombre_archivo}"
            )
        except Exception as e:
            logger.error(
                f"Worker [{self.worker_id}]: Error leyendo archivo {nombre_archivo}: {e}"
            )

        return None

    def _actualizar_saldos_archivo(
        self,
        id_particion: str,
        id_cuenta_origen: int,
        nuevo_saldo_origen: float,
        id_cuenta_destino: int,
        nuevo_saldo_destino: float,
    ) -> bool:
        """Actualiza los saldos de dos cuentas en el archivo de partición"""
        nombre_archivo = os.path.join(self.directorio_base_datos, f"{id_particion}.txt")
        nombre_archivo_temp = f"{nombre_archivo}.tmp"

        try:
            # Leer todas las líneas
            with open(nombre_archivo, "r", encoding="utf-8") as f:
                lineas = f.readlines()

            origen_actualizado = False
            destino_actualizado = False

            # Escribir archivo temporal con actualizaciones
            with open(nombre_archivo_temp, "w", encoding="utf-8") as f:
                for i, linea in enumerate(lineas):
                    if i == 0:  # Cabecera
                        f.write(linea)
                        continue

                    partes = linea.strip().split("|")
                    if len(partes) >= 4:
                        try:
                            id_cuenta_actual = int(partes[0])
                            if id_cuenta_actual == id_cuenta_origen:
                                f.write(
                                    f"{id_cuenta_actual}|{partes[1]}|{nuevo_saldo_origen:.2f}|{partes[3]}\n"
                                )
                                origen_actualizado = True
                            elif id_cuenta_actual == id_cuenta_destino:
                                f.write(
                                    f"{id_cuenta_actual}|{partes[1]}|{nuevo_saldo_destino:.2f}|{partes[3]}\n"
                                )
                                destino_actualizado = True
                            else:
                                f.write(linea)
                        except ValueError:
                            f.write(linea)
                    else:
                        f.write(linea)

            # Verificar que ambas cuentas fueron actualizadas
            if not origen_actualizado:
                logger.error(
                    f"Worker [{self.worker_id}]: Cuenta origen {id_cuenta_origen} no encontrada"
                )
                os.remove(nombre_archivo_temp)
                return False

            if not destino_actualizado:
                logger.error(
                    f"Worker [{self.worker_id}]: Cuenta destino {id_cuenta_destino} no encontrada"
                )
                os.remove(nombre_archivo_temp)
                return False

            # Reemplazar archivo original
            os.replace(nombre_archivo_temp, nombre_archivo)
            return True

        except Exception as e:
            logger.error(
                f"Worker [{self.worker_id}]: Error actualizando saldos en {nombre_archivo}: {e}"
            )
            if os.path.exists(nombre_archivo_temp):
                os.remove(nombre_archivo_temp)
            return False

    def _actualizar_saldo_unica_cuenta(
        self, id_particion: str, id_cuenta: int, nuevo_saldo: float, operacion_desc: str
    ) -> bool:
        """Actualiza el saldo de una sola cuenta en el archivo de partición"""
        nombre_archivo = os.path.join(self.directorio_base_datos, f"{id_particion}.txt")
        nombre_archivo_temp = f"{nombre_archivo}.tmp"

        try:
            # Leer todas las líneas
            with open(nombre_archivo, "r", encoding="utf-8") as f:
                lineas = f.readlines()

            cuenta_actualizada = False

            # Escribir archivo temporal con actualizaciones
            with open(nombre_archivo_temp, "w", encoding="utf-8") as f:
                for i, linea in enumerate(lineas):
                    if i == 0:  # Cabecera
                        f.write(linea)
                        continue

                    partes = linea.strip().split("|")
                    if len(partes) >= 4:
                        try:
                            id_cuenta_actual = int(partes[0])
                            if id_cuenta_actual == id_cuenta:
                                f.write(
                                    f"{id_cuenta_actual}|{partes[1]}|{nuevo_saldo:.2f}|{partes[3]}\n"
                                )
                                cuenta_actualizada = True
                            else:
                                f.write(linea)
                        except ValueError:
                            f.write(linea)
                    else:
                        f.write(linea)

            if not cuenta_actualizada:
                logger.error(
                    f"Worker [{self.worker_id}]: Cuenta {id_cuenta} no encontrada para {operacion_desc}"
                )
                os.remove(nombre_archivo_temp)
                return False

            # Reemplazar archivo original
            os.replace(nombre_archivo_temp, nombre_archivo)
            logger.info(
                f"Worker [{self.worker_id}]: Saldo actualizado para cuenta {id_cuenta} en {id_particion} ({operacion_desc}). Nuevo saldo: {nuevo_saldo}"
            )
            return True

        except Exception as e:
            logger.error(
                f"Worker [{self.worker_id}]: Error actualizando saldo en {nombre_archivo} para {operacion_desc}: {e}"
            )
            if os.path.exists(nombre_archivo_temp):
                os.remove(nombre_archivo_temp)
            return False

    def _registrar_transaccion_local(
        self,
        id_transaccion_global: int,
        id_cuenta_origen: int,
        id_cuenta_destino: int,
        monto: float,
        estado_detalle: str,
    ):
        """Registra una transacción en el log local"""
        try:
            with open(self.archivo_log_transacciones, "a", encoding="utf-8") as f:
                timestamp = datetime.now().isoformat()
                f.write(
                    f"TxGlobal:{id_transaccion_global}|{id_cuenta_origen}|{id_cuenta_destino}|{monto:.2f}|{timestamp}|{estado_detalle}\n"
                )
        except Exception as e:
            logger.error(
                f"Worker [{self.worker_id}]: Error escribiendo log de transacciones: {e}"
            )


def main():
    if len(sys.argv) < 4:
        print(
            "Uso: python nodo_trabajador.py <workerId> <hostServidorCentral> <puertoServidorCentral> <puertoEscuchaTareas>"
        )
        print("Ejemplo: python nodo_trabajador.py worker_py1 localhost 12346 12351")
        sys.exit(1)

    worker_id = sys.argv[1]
    host_servidor = sys.argv[2]
    puerto_servidor = int(sys.argv[3])
    puerto_tareas = int(sys.argv[4])

    worker = NodoTrabajadorPython(
        worker_id, host_servidor, puerto_servidor, puerto_tareas
    )
    worker.iniciar()


if __name__ == "__main__":
    main()

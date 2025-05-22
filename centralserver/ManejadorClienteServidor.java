package centralserver;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
// import java.util.StringBuilder;
// import commons.*; // Importará las clases de commons cuando sea necesario
import commons.Solicitud; // Importar Solicitud
import commons.Respuesta; // Importar Respuesta
import commons.EstadoOperacion; // Importar EstadoOperacion
import commons.TipoOperacion;
import commons.InfoWorker; // Importar InfoWorker

// package centralserver; // Eliminado para simplificar

public class ManejadorClienteServidor implements Runnable {
        private Socket socketCliente;
        private ObjectOutputStream oos;
        private ObjectInputStream ois;
        private boolean activo;
        private ServidorCentral servidorCentralInstance; // Necesario para acceder a métodos no estáticos si es el caso

        public ManejadorClienteServidor(Socket socketCliente, ServidorCentral servidorCentralInstance) {
                this.socketCliente = socketCliente;
                this.servidorCentralInstance = servidorCentralInstance; // Guardar la instancia
                this.activo = true;
                try {
                        // Importante: Crear ObjectOutputStream ANTES de ObjectInputStream para evitar
                        // bloqueos
                        this.oos = new ObjectOutputStream(socketCliente.getOutputStream());
                        this.ois = new ObjectInputStream(socketCliente.getInputStream());
                        System.out.println("ManejadorCliente: Streams creados para " + socketCliente.getInetAddress());
                } catch (IOException e) {
                        System.err.println("ManejadorCliente: Error al crear streams para "
                                        + socketCliente.getInetAddress() + ": "
                                        + e.getMessage());
                        activo = false;
                        cerrarSocket();
                }
        }

        @Override
        public void run() {
                if (!activo) {
                        return; // No hacer nada si los streams no se inicializaron correctamente
                }
                System.out.println(
                                "Manejador iniciado para cliente: " + socketCliente.getInetAddress().getHostAddress());

                try {
                        while (activo) {
                                Object objetoRecibido = ois.readObject();
                                if (objetoRecibido instanceof Solicitud) {
                                        Solicitud solicitud = (Solicitud) objetoRecibido;
                                        System.out.println("Solicitud recibida de ["
                                                        + socketCliente.getInetAddress().getHostAddress()
                                                        + "]: " + solicitud.getTipoOperacion() + " con parámetros: "
                                                        + solicitud.getParametros());

                                        Respuesta respuestaCliente;
                                        if (solicitud.getTipoOperacion() == TipoOperacion.CONSULTAR_SALDO) {
                                                respuestaCliente = procesarConsultaSaldo(solicitud);
                                        } else if (solicitud.getTipoOperacion() == TipoOperacion.TRANSFERIR_FONDOS) {
                                                respuestaCliente = procesarTransferenciaFondos(solicitud);
                                        } else if (solicitud.getTipoOperacion() == TipoOperacion.ARQUEO_CUENTAS) {
                                                respuestaCliente = procesarArqueoCuentas(solicitud);
                                        } else {
                                                respuestaCliente = new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                                                "Operación no reconocida por el servidor.", null);
                                        }
                                        enviarRespuesta(respuestaCliente);
                                } else {
                                        System.err.println(
                                                        "Objeto recibido no es de tipo Solicitud: " + objetoRecibido);
                                        enviarRespuesta(
                                                        new Respuesta(EstadoOperacion.ERROR_COMUNICACION,
                                                                        "Formato de solicitud incorrecto", null));
                                }
                        }
                } catch (EOFException e) {
                        System.out.println(
                                        "ManejadorCliente: Cliente " + socketCliente.getInetAddress().getHostAddress()
                                                        + " cerró la conexión.");
                } catch (IOException e) {
                        if (activo && !socketCliente.isClosed()) { // Solo mostrar error si no fue un cierre esperado o
                                                                   // provocado
                                                                   // por nosotros
                                System.err.println("ManejadorCliente: Error de IOException con cliente "
                                                + socketCliente.getInetAddress().getHostAddress() + ": "
                                                + e.getMessage());
                        }
                } catch (ClassNotFoundException e) {
                        System.err.println("ManejadorCliente: Error ClassNotFoundException con cliente "
                                        + socketCliente.getInetAddress().getHostAddress() + ": " + e.getMessage());
                } finally {
                        cerrarRecursos();
                }
        }

        private Respuesta procesarConsultaSaldo(Solicitud solicitudCliente) {
                System.out.println("ManejadorCliente: Procesando CONSULTAR_SALDO para: "
                                + solicitudCliente.getParametros());
                Integer idCuenta = (Integer) solicitudCliente.getParametros().get("ID_CUENTA");

                if (idCuenta == null) {
                        return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                        "ID_CUENTA no proporcionado en la solicitud.",
                                        null);
                }

                String idParticion = ServidorCentral.cuentaAParticion.get(idCuenta);
                if (idParticion == null) {
                        System.err.println("ManejadorCliente: No se encontró partición para ID_CUENTA: " + idCuenta);
                        return new Respuesta(EstadoOperacion.ERROR_CUENTA_ORIGEN_NO_EXISTE,
                                        "Cuenta (" + idCuenta + ") no encontrada o no mapeada a una partición.", null);
                }
                System.out.println("ManejadorCliente: ID_CUENTA " + idCuenta + " pertenece a partición " + idParticion);

                List<String> idsWorkersConParticion = ServidorCentral.particionANodos.get(idParticion);
                if (idsWorkersConParticion == null || idsWorkersConParticion.isEmpty()) {
                        System.err.println("ManejadorCliente: No hay workers registrados para la partición "
                                        + idParticion);
                        return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                        "No hay workers disponibles para la partición " + idParticion, null);
                }

                // Copiar la lista para iterar de forma segura si la original se modifica (ej.
                // desregistrarWorker)
                List<String> workersAIterar = new ArrayList<>(idsWorkersConParticion);

                for (String workerId : workersAIterar) {
                        InfoWorker infoWorker = ServidorCentral.workersActivos.get(workerId);
                        if (infoWorker != null && infoWorker.isActivo()) {
                                System.out.println("ManejadorCliente: Intentando CONSULTAR_SALDO de cta " + idCuenta
                                                + " en worker "
                                                + workerId + " (" + infoWorker.getHost() + ":"
                                                + infoWorker.getPuertoTareas() + ")");

                                try (Socket socketAlWorker = new Socket(infoWorker.getHost(),
                                                infoWorker.getPuertoTareas());
                                                ObjectOutputStream oosWorker = new ObjectOutputStream(
                                                                socketAlWorker.getOutputStream());
                                                ObjectInputStream oisWorker = new ObjectInputStream(
                                                                socketAlWorker.getInputStream())) {

                                        socketAlWorker.setSoTimeout(10000); // Timeout de 10 segundos para la operación
                                                                            // con el worker

                                        // Crear una Solicitud para el worker (no MensajeWorker tipo NUEVA_TAREA aquí,
                                        // ya que el worker escucha directamente Solicitud)
                                        Map<String, Object> paramsParaWorker = Map.of("ID_CUENTA", idCuenta,
                                                        "ID_PARTICION", idParticion);
                                        Solicitud solicitudAWorker = new Solicitud(TipoOperacion.CONSULTAR_SALDO,
                                                        paramsParaWorker);

                                        oosWorker.writeObject(solicitudAWorker);
                                        oosWorker.flush();
                                        System.out.println(
                                                        "ManejadorCliente: Solicitud CONSULTAR_SALDO enviada a worker "
                                                                        + workerId);

                                        Object respuestaObj = oisWorker.readObject();
                                        if (respuestaObj instanceof Respuesta) {
                                                Respuesta respuestaDeWorker = (Respuesta) respuestaObj;
                                                System.out.println("ManejadorCliente: Respuesta recibida de worker "
                                                                + workerId + ": "
                                                                + respuestaDeWorker.getEstado() + " - "
                                                                + respuestaDeWorker.getMensaje());

                                                // Si el worker procesó la solicitud (con éxito o error de negocio),
                                                // usamos esa
                                                // respuesta.
                                                if (respuestaDeWorker.getEstado() == EstadoOperacion.EXITO ||
                                                                respuestaDeWorker
                                                                                .getEstado() == EstadoOperacion.ERROR_CUENTA_ORIGEN_NO_EXISTE) {
                                                        return respuestaDeWorker;
                                                } else {
                                                        System.err.println("ManejadorCliente: Worker " + workerId
                                                                        + " devolvió un error funcional: "
                                                                        + respuestaDeWorker.getEstado() + " - "
                                                                        + respuestaDeWorker.getMensaje());
                                                        // Si es un error general del worker, podríamos intentar con
                                                        // otro si la lista
                                                        // 'workersAIterar' tuviera más y estuviéramos en un bucle de
                                                        // reintento.
                                                        // Por ahora, si no es EXITO o CUENTA_NO_EXISTE (que es una
                                                        // respuesta válida),
                                                        // devolvemos el error del worker.
                                                        return respuestaDeWorker;
                                                }
                                        } else {
                                                System.err.println("ManejadorCliente: Respuesta inesperada de worker "
                                                                + workerId + ". Tipo: "
                                                                + respuestaObj.getClass().getName());
                                                // Continuar al siguiente worker si hay un error de formato de respuesta
                                        }

                                } catch (SocketTimeoutException e) {
                                        System.err.println("ManejadorCliente: Timeout al comunicarse con worker "
                                                        + workerId
                                                        + " para CONSULTAR_SALDO. Desregistrando worker.");
                                        ServidorCentral.desregistrarWorker(workerId); // Asumir que el worker está caído
                                                                                      // o inaccesible
                                        // Continuar al siguiente worker en la lista
                                } catch (IOException e) {
                                        System.err.println("ManejadorCliente: Error de IO al comunicarse con worker "
                                                        + workerId + ": "
                                                        + e.getMessage() + ". Desregistrando worker.");
                                        ServidorCentral.desregistrarWorker(workerId); // Asumir que el worker está caído
                                        // Continuar al siguiente worker en la lista
                                } catch (ClassNotFoundException e) {
                                        System.err.println(
                                                        "ManejadorCliente: Error ClassNotFound al recibir respuesta de worker "
                                                                        + workerId + ": " + e.getMessage());
                                        // Esto es un error grave, no continuar con otros workers para esta solicitud.
                                        return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                                        "Error de formato en respuesta del worker (ClassNotFound)",
                                                        null);
                                }
                        } else {
                                System.out.println("ManejadorCliente: Worker " + workerId
                                                + " no encontrado en activos o marcado como inactivo. Intentando siguiente si hay.");
                        }
                }
                // Si se intentó con todos los workers disponibles para la partición y ninguno
                // respondió exitosamente o dio una respuesta definitiva
                System.err.println("ManejadorCliente: No se pudo completar CONSULTAR_SALDO para cuenta " + idCuenta
                                + " en partición " + idParticion + " tras intentar todos los workers asignados.");
                return new Respuesta(EstadoOperacion.ERROR_REINTENTAR_EN_OTRO_NODO,
                                "No se pudo contactar a un worker funcional para la partición " + idParticion
                                                + " o todos fallaron.",
                                null);
        }

        private Respuesta procesarTransferenciaFondos(Solicitud solicitudCliente) {
                System.out.println("ManejadorCliente: Procesando TRANSFERIR_FONDOS para: "
                                + solicitudCliente.getParametros());
                Integer idCuentaOrigen = (Integer) solicitudCliente.getParametros().get("ID_CUENTA_ORIGEN");
                Integer idCuentaDestino = (Integer) solicitudCliente.getParametros().get("ID_CUENTA_DESTINO");
                Double monto = (Double) solicitudCliente.getParametros().get("MONTO");
                int idTransaccionGlobal = ServidorCentral.generarIdTransaccionGlobal(); // Generar ID al inicio

                if (idCuentaOrigen == null || idCuentaDestino == null || monto == null) {
                        ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal,
                                        idCuentaOrigen != null ? idCuentaOrigen : -1,
                                        idCuentaDestino != null ? idCuentaDestino : -1,
                                        monto != null ? monto : 0.0, EstadoOperacion.TRANSACCION_FALLIDA,
                                        "Parámetros incompletos");
                        return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                        "Parámetros incompletos para TRANSFERIR_FONDOS.", null);
                }
                if (monto <= 0) {
                        ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal, idCuentaOrigen, idCuentaDestino,
                                        monto,
                                        EstadoOperacion.TRANSACCION_FALLIDA, "Monto inválido");
                        return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                        "Monto de transferencia debe ser positivo.",
                                        null);
                }
                if (idCuentaOrigen.equals(idCuentaDestino)) {
                        ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal, idCuentaOrigen, idCuentaDestino,
                                        monto,
                                        EstadoOperacion.TRANSACCION_FALLIDA, "Cuentas origen y destino iguales");
                        return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                        "Cuenta origen y destino no pueden ser la misma.", null);
                }

                String particionOrigen = ServidorCentral.cuentaAParticion.get(idCuentaOrigen);
                String particionDestino = ServidorCentral.cuentaAParticion.get(idCuentaDestino);

                if (particionOrigen == null) {
                        ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal, idCuentaOrigen, idCuentaDestino,
                                        monto,
                                        EstadoOperacion.ERROR_CUENTA_ORIGEN_NO_EXISTE,
                                        "Partición origen no encontrada");
                        return new Respuesta(EstadoOperacion.ERROR_CUENTA_ORIGEN_NO_EXISTE,
                                        "Cuenta origen (" + idCuentaOrigen + ") no mapeada a partición.", null);
                }
                if (particionDestino == null) {
                        ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal, idCuentaOrigen, idCuentaDestino,
                                        monto,
                                        EstadoOperacion.ERROR_CUENTA_DESTINO_NO_EXISTE,
                                        "Partición destino no encontrada");
                        return new Respuesta(EstadoOperacion.ERROR_CUENTA_DESTINO_NO_EXISTE,
                                        "Cuenta destino (" + idCuentaDestino + ") no mapeada a partición.", null);
                }

                System.out.println("ManejadorCliente: Transacción ID: " + idTransaccionGlobal + " - Cta Origen "
                                + idCuentaOrigen + " (Part. " + particionOrigen +
                                "), Cta Destino " + idCuentaDestino + " (Part. " + particionDestino + "), Monto "
                                + monto);

                if (particionOrigen.equals(particionDestino)) {
                        // Caso A: Misma partición
                        return manejarTransferenciaMismaParticion(idTransaccionGlobal, idCuentaOrigen, idCuentaDestino,
                                        monto,
                                        particionOrigen);
                } else {
                        // Caso B: Diferentes particiones
                        return manejarTransferenciaDiferentesParticiones(idTransaccionGlobal, idCuentaOrigen,
                                        particionOrigen,
                                        idCuentaDestino, particionDestino, monto);
                }
        }

        private Respuesta manejarTransferenciaMismaParticion(int idTransaccionGlobal, int idCtaOrigen, int idCtaDestino,
                        double monto, String idParticion) {
                System.out.println("ManejadorCliente [Tx:" + idTransaccionGlobal + "]: Transferencia intra-partición ("
                                + idParticion + ").");
                List<String> idsWorkers = ServidorCentral.particionANodos.get(idParticion);
                if (idsWorkers == null || idsWorkers.isEmpty()) {
                        ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal, idCtaOrigen, idCtaDestino,
                                        monto,
                                        EstadoOperacion.TRANSACCION_FALLIDA,
                                        "No workers para partición " + idParticion);
                        return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                        "No hay workers para partición " + idParticion,
                                        null);
                }

                List<String> workersAIterar = new ArrayList<>(idsWorkers);
                for (String workerId : workersAIterar) {
                        InfoWorker infoWorker = ServidorCentral.workersActivos.get(workerId);
                        if (infoWorker != null && infoWorker.isActivo()) {
                                System.out.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                                + "]: Intentando TRANSFERIR_FONDOS (intra-partición) en worker "
                                                + workerId);
                                try (Socket socketAlWorker = new Socket(infoWorker.getHost(),
                                                infoWorker.getPuertoTareas());
                                                ObjectOutputStream oosWorker = new ObjectOutputStream(
                                                                socketAlWorker.getOutputStream());
                                                ObjectInputStream oisWorker = new ObjectInputStream(
                                                                socketAlWorker.getInputStream())) {
                                        socketAlWorker.setSoTimeout(15000);
                                        Map<String, Object> params = Map.of("ID_CUENTA_ORIGEN", idCtaOrigen,
                                                        "ID_CUENTA_DESTINO",
                                                        idCtaDestino, "MONTO", monto, "ID_PARTICION", idParticion,
                                                        "ID_TRANSACCION_GLOBAL",
                                                        idTransaccionGlobal);
                                        Solicitud solicitudAWorker = new Solicitud(TipoOperacion.TRANSFERIR_FONDOS,
                                                        params);
                                        oosWorker.writeObject(solicitudAWorker);
                                        oosWorker.flush();

                                        Object respuestaObj = oisWorker.readObject();
                                        if (respuestaObj instanceof Respuesta) {
                                                Respuesta respuestaDeWorker = (Respuesta) respuestaObj;
                                                System.out.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                                                + "]: Respuesta de TRANSFERIR_FONDOS de worker "
                                                                + workerId + ": "
                                                                + respuestaDeWorker.getEstado());

                                                String detalleLog = "Worker: " + workerId + ", Particion: "
                                                                + idParticion + ". " + respuestaDeWorker.getMensaje();
                                                if (respuestaDeWorker.getDatos() instanceof Map) {
                                                        detalleLog += " Datos: "
                                                                        + respuestaDeWorker.getDatos().toString();
                                                }
                                                ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal,
                                                                idCtaOrigen, idCtaDestino, monto,
                                                                respuestaDeWorker.getEstado(), detalleLog);

                                                if (respuestaDeWorker.getEstado() == EstadoOperacion.EXITO
                                                                && respuestaDeWorker.getDatos() instanceof Map) {
                                                        @SuppressWarnings("unchecked")
                                                        Map<String, Object> datosResultado = (Map<String, Object>) respuestaDeWorker
                                                                        .getDatos();
                                                        Double nuevoSaldoOrigen = (Double) datosResultado
                                                                        .get("nuevoSaldoOrigen");
                                                        Double nuevoSaldoDestino = (Double) datosResultado
                                                                        .get("nuevoSaldoDestino");
                                                        if (nuevoSaldoOrigen != null) {
                                                                replicarActualizacionSaldo(idParticion, idCtaOrigen,
                                                                                nuevoSaldoOrigen, idTransaccionGlobal,
                                                                                workerId);
                                                        }
                                                        if (nuevoSaldoDestino != null) {
                                                                replicarActualizacionSaldo(idParticion, idCtaDestino,
                                                                                nuevoSaldoDestino, idTransaccionGlobal,
                                                                                workerId);
                                                        }
                                                }
                                                return respuestaDeWorker;
                                        } else {
                                                System.err.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                                                + "]: Respuesta inesperada (TRANSFERIR_FONDOS) de worker "
                                                                + workerId);
                                                ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal,
                                                                idCtaOrigen, idCtaDestino, monto,
                                                                EstadoOperacion.ERROR_COMUNICACION,
                                                                "Respuesta inesperada del worker " + workerId);
                                        } // No se retorna aquí para poder intentar con otro worker si este da una
                                          // respuesta malformada
                                } catch (SocketTimeoutException e) {
                                        System.err.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                                        + "]: Timeout con worker " + workerId
                                                        + " para TRANSFERIR_FONDOS (intra-partición). Desregistrando.");
                                        ServidorCentral.desregistrarWorker(workerId);
                                } catch (IOException e) {
                                        System.err.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                                        + "]: Error IO con worker " + workerId
                                                        + " para TRANSFERIR_FONDOS (intra-partición): " + e.getMessage()
                                                        + ". Desregistrando.");
                                        ServidorCentral.desregistrarWorker(workerId);
                                } catch (ClassNotFoundException e) {
                                        System.err.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                                        + "]: Error ClassNotFound (TRANSFERIR_FONDOS) de worker "
                                                        + workerId + ": " + e.getMessage());
                                        ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal, idCtaOrigen,
                                                        idCtaDestino, monto, EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                                        "Error de formato en respuesta del worker (ClassNotFound) para Tx:"
                                                                        + idTransaccionGlobal);
                                        return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                                        "Error de formato en respuesta del worker.", null); // Error
                                                                                                            // grave, no
                                                                                                            // reintentar
                                }
                        }
                }
                ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal, idCtaOrigen, idCtaDestino, monto,
                                EstadoOperacion.TRANSACCION_FALLIDA,
                                "Ningún worker pudo procesar transferencia intra-partición " + idParticion);
                return new Respuesta(EstadoOperacion.ERROR_REINTENTAR_EN_OTRO_NODO,
                                "No se pudo completar TRANSFERIR_FONDOS en partición " + idParticion
                                                + " con ningún worker.",
                                null);
        }

        private Respuesta manejarTransferenciaDiferentesParticiones(int idTransaccionGlobal, int idCtaOrigen,
                        String pOrigen, int idCtaDestino, String pDestino, double monto) {
                System.out.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                + "]: Transferencia inter-partición (Origen:" + pOrigen + ", Destino:" + pDestino
                                + ")");
                Respuesta respFinalCliente;
                String workerOrigenId = null, workerDestinoId = null;
                Double saldoFinalOrigen = null, saldoFinalDestino = null;

                // Paso 1: Preparar Débito
                Respuesta respPrepDebito = enviarSolicitudSubOperacion(pOrigen, TipoOperacion.PREPARAR_DEBITO,
                                Map.of("ID_CUENTA_ORIGEN", idCtaOrigen, "MONTO", monto, "ID_TRANSACCION_GLOBAL",
                                                idTransaccionGlobal, "ID_PARTICION", pOrigen),
                                "PREPARAR_DEBITO Tx:" + idTransaccionGlobal);
                if (respPrepDebito.getDatos() instanceof Map) {
                        workerOrigenId = ((Map<String, String>) respPrepDebito.getDatos()).get("workerIdProcesador"); // Asumimos
                                                                                                                      // que
                                                                                                                      // el
                                                                                                                      // worker
                                                                                                                      // lo
                                                                                                                      // incluye
                }

                if (respPrepDebito.getEstado() != EstadoOperacion.DEBITO_PREPARADO_OK) {
                        ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal, idCtaOrigen, idCtaDestino,
                                        monto, respPrepDebito.getEstado(),
                                        "Fallo PREPARAR_DEBITO: " + respPrepDebito.getMensaje());
                        return respPrepDebito;
                }
                System.out.println("ManejadorCliente [Tx:" + idTransaccionGlobal + "]: Débito PREPARADO en part "
                                + pOrigen + " por worker " + workerOrigenId + ". Saldo origen antes: "
                                + ((Map<String, Object>) respPrepDebito.getDatos()).get("saldoActualOrigen"));
                saldoFinalOrigen = ((Double) ((Map<String, Object>) respPrepDebito.getDatos()).get("saldoActualOrigen"))
                                - monto;

                // Paso 2: Aplicar Crédito
                Respuesta respAplicarCredito = enviarSolicitudSubOperacion(pDestino, TipoOperacion.APLICAR_CREDITO,
                                Map.of("ID_CUENTA_DESTINO", idCtaDestino, "MONTO", monto, "ID_TRANSACCION_GLOBAL",
                                                idTransaccionGlobal, "ID_PARTICION", pDestino),
                                "APLICAR_CREDITO Tx:" + idTransaccionGlobal);
                if (respAplicarCredito.getDatos() instanceof Map) {
                        workerDestinoId = ((Map<String, String>) respAplicarCredito.getDatos())
                                        .get("workerIdProcesador");
                        saldoFinalDestino = (Double) ((Map<String, Object>) respAplicarCredito.getDatos())
                                        .get("nuevoSaldoDestino");
                }

                if (respAplicarCredito.getEstado() != EstadoOperacion.CREDITO_APLICADO_OK) {
                        System.err.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                        + "]: Falló APLICAR_CREDITO en part " + pDestino
                                        + ". Revertiendo débito en part " + pOrigen);
                        ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal, idCtaOrigen, idCtaDestino,
                                        monto, EstadoOperacion.TRANSACCION_FALLIDA, "Fallo APLICAR_CREDITO ("
                                                        + respAplicarCredito.getMensaje() + "). Reintentando débito.");

                        Respuesta respReversion = enviarSolicitudSubOperacion(pOrigen, TipoOperacion.REVERTIR_DEBITO,
                                        Map.of("ID_CUENTA_ORIGEN", idCtaOrigen, "MONTO", monto, "ID_TRANSACCION_GLOBAL",
                                                        idTransaccionGlobal, "ID_PARTICION", pOrigen),
                                        "REVERTIR_DEBITO Tx:" + idTransaccionGlobal);
                        if (respReversion.getEstado() != EstadoOperacion.DEBITO_REVERTIDO_OK) {
                                System.err.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                                + "]: CRÍTICO - Falló REVERTIR_DEBITO. INCONSISTENCIA.");
                                ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal, idCtaOrigen,
                                                idCtaDestino, monto, EstadoOperacion.TRANSACCION_FALLIDA,
                                                "CRITICO: Fallo APLICAR_CREDITO y Fallo REVERTIR_DEBITO. Requiere intervención.");
                                respFinalCliente = new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                                "Error crítico: Fallo al aplicar crédito y al revertir débito.", null);
                        } else {
                                System.out.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                                + "]: Débito REVERTIDO en part " + pOrigen);
                                ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal, idCtaOrigen,
                                                idCtaDestino, monto, EstadoOperacion.TRANSACCION_FALLIDA,
                                                "Fallo APLICAR_CREDITO, Débito Revertido.");
                                respFinalCliente = new Respuesta(respAplicarCredito.getEstado(),
                                                "Fallo al aplicar crédito: " + respAplicarCredito.getMensaje()
                                                                + ". Débito revertido.",
                                                null);
                        }
                        return respFinalCliente;
                }
                System.out.println("ManejadorCliente [Tx:" + idTransaccionGlobal + "]: Crédito APLICADO en part "
                                + pDestino + " por worker " + workerDestinoId);

                // Paso 3: Confirmar Débito
                Respuesta respConfirmarDebito = enviarSolicitudSubOperacion(pOrigen, TipoOperacion.CONFIRMAR_DEBITO,
                                Map.of("ID_CUENTA_ORIGEN", idCtaOrigen, "MONTO", monto, "ID_TRANSACCION_GLOBAL",
                                                idTransaccionGlobal, "ID_PARTICION", pOrigen),
                                "CONFIRMAR_DEBITO Tx:" + idTransaccionGlobal);

                if (respConfirmarDebito.getEstado() != EstadoOperacion.DEBITO_CONFIRMADO_OK) {
                        System.err.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                        + "]: CRÍTICO - Falló CONFIRMAR_DEBITO en part " + pOrigen
                                        + " DESPUÉS de aplicar crédito. INCONSISTENCIA GRAVE.");
                        ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal, idCtaOrigen, idCtaDestino,
                                        monto, EstadoOperacion.TRANSACCION_FALLIDA,
                                        "CRITICO: Crédito aplicado, pero Fallo CONFIRMAR_DEBITO. Requiere intervención urgente.");
                        // NO se revierte el crédito aquí, ya que es más complejo y podría fallar
                        // también.
                        // Se prioriza no perder dinero, aunque la cuenta origen no refleje el débito
                        // final.
                        return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                        "Error crítico: Fallo al confirmar débito tras aplicar crédito. Fondos acreditados pero débito no confirmado.",
                                        null);
                }
                // Actualizar saldoFinalOrigen con el dato del worker que confirmó
                if (respConfirmarDebito.getDatos() instanceof Map) {
                        saldoFinalOrigen = (Double) ((Map<String, Object>) respConfirmarDebito.getDatos())
                                        .get("nuevoSaldoOrigen");
                }
                System.out.println("ManejadorCliente [Tx:" + idTransaccionGlobal + "]: Débito CONFIRMADO en part "
                                + pOrigen + ". Worker: "
                                + ((Map<String, String>) respConfirmarDebito.getDatos()).get("workerIdProcesador"));

                ServidorCentral.registrarTransaccionGlobal(idTransaccionGlobal, idCtaOrigen, idCtaDestino, monto,
                                EstadoOperacion.TRANSACCION_CONFIRMADA, "Transferencia inter-partición completada.");

                // Replicar cambios
                if (saldoFinalOrigen != null)
                        replicarActualizacionSaldo(pOrigen, idCtaOrigen, saldoFinalOrigen, idTransaccionGlobal,
                                        workerOrigenId);
                if (saldoFinalDestino != null)
                        replicarActualizacionSaldo(pDestino, idCtaDestino, saldoFinalDestino, idTransaccionGlobal,
                                        workerDestinoId);

                return new Respuesta(EstadoOperacion.EXITO,
                                "Transferencia inter-partición completada (TxID: " + idTransaccionGlobal + ")", null);
        }

        private Respuesta procesarArqueoCuentas(Solicitud solicitudArqueo) {
                System.out.println("ManejadorCliente: Procesando ARQUEO_CUENTAS...");
                double saldoTotalSistema = 0.0;
                int particionesConsultadas = 0;
                int particionesConError = 0;
                StringBuilder detallesErrores = new StringBuilder();

                // Iterar sobre todas las particiones de cuentas definidas globalmente
                // Necesitamos obtener los IDs de las particiones de cuentas (ej. "CUENTA_P1",
                // "CUENTA_P2", etc.)
                // Esto podría obtenerse de las claves de datosParticionesGlobales que son de
                // tipo Cuenta
                // o tener una lista predefinida de IDs de particiones de cuentas.

                List<String> idsParticionesCuenta = ServidorCentral.datosParticionesGlobales.keySet().stream()
                                .filter(id -> id.startsWith("CUENTA_P"))
                                .collect(Collectors.toList());

                if (idsParticionesCuenta.isEmpty()) {
                        return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                        "No hay particiones de cuentas definidas para el arqueo.", null);
                }

                for (String idParticion : idsParticionesCuenta) {
                        Map<String, Object> paramsParaWorker = Map.of("ID_PARTICION", idParticion);
                        // Usamos enviarSolicitudSubOperacion para pedirle a un worker de esa partición
                        // que calcule su total
                        Respuesta respWorker = enviarSolicitudSubOperacion(idParticion,
                                        TipoOperacion.CALCULAR_SALDO_PARTICION, paramsParaWorker,
                                        "ARQUEO_PARTICION_" + idParticion);

                        if (respWorker.getEstado() == EstadoOperacion.EXITO
                                        && respWorker.getDatos() instanceof Double) {
                                saldoTotalSistema += (Double) respWorker.getDatos();
                                particionesConsultadas++;
                        } else {
                                particionesConError++;
                                String errorMsg = "Error al obtener saldo de partición " + idParticion + ": "
                                                + respWorker.getMensaje() + " (Estado: " + respWorker.getEstado() + ")";
                                System.err.println("ManejadorCliente: " + errorMsg);
                                detallesErrores.append(errorMsg).append("; ");
                                // No detenemos el arqueo, intentamos sumar lo que se pueda
                        }
                }

                String mensajeFinal = "Arqueo completado. Total de " + particionesConsultadas + " particiones sumadas.";
                if (particionesConError > 0) {
                        mensajeFinal += " " + particionesConError + " particiones no pudieron ser sumadas. Detalles: "
                                        + detallesErrores.toString();
                        return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR, mensajeFinal, saldoTotalSistema);
                } else {
                        return new Respuesta(EstadoOperacion.EXITO, mensajeFinal, saldoTotalSistema);
                }
        }

        private Respuesta enviarSolicitudSubOperacion(String idParticion, TipoOperacion tipoSubOperacion,
                        Map<String, Object> parametros, String logContext) {
                List<String> idsWorkers = ServidorCentral.particionANodos.get(idParticion);
                if (idsWorkers == null || idsWorkers.isEmpty()) {
                        System.err.println("ManejadorCliente [" + logContext + "]: No hay workers para partición "
                                        + idParticion);
                        return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                        "No workers para partición " + idParticion, null);
                }
                List<String> workersAIterar = new ArrayList<>(idsWorkers);
                for (String workerId : workersAIterar) {
                        InfoWorker infoWorker = ServidorCentral.workersActivos.get(workerId);
                        if (infoWorker != null && infoWorker.isActivo()) {
                                System.out.println("ManejadorCliente [" + logContext + "]: Intentando "
                                                + tipoSubOperacion + " en worker " + workerId + " ("
                                                + infoWorker.getHost() + ":" + infoWorker.getPuertoTareas() + ")");
                                try (Socket socketAlWorker = new Socket(infoWorker.getHost(),
                                                infoWorker.getPuertoTareas());
                                                ObjectOutputStream oosWorker = new ObjectOutputStream(
                                                                socketAlWorker.getOutputStream());
                                                ObjectInputStream oisWorker = new ObjectInputStream(
                                                                socketAlWorker.getInputStream())) {
                                        socketAlWorker.setSoTimeout(10000); // Timeout para sub-operaciones
                                        Solicitud solicitudAWorker = new Solicitud(tipoSubOperacion, parametros);
                                        oosWorker.writeObject(solicitudAWorker);
                                        oosWorker.flush();
                                        Object respuestaObj = oisWorker.readObject();
                                        if (respuestaObj instanceof Respuesta) {
                                                Respuesta resp = (Respuesta) respuestaObj;
                                                // Añadir el workerId que procesó a los datos de la respuesta para
                                                // trazabilidad
                                                Map<String, Object> datosOriginales = resp.getDatos() instanceof Map
                                                                ? (Map<String, Object>) resp.getDatos()
                                                                : new HashMap<>();
                                                Map<String, Object> datosConWorkerId = new HashMap<>(datosOriginales);
                                                datosConWorkerId.put("workerIdProcesador", workerId);
                                                resp.setDatos(datosConWorkerId);
                                                return resp;
                                        }
                                        System.err.println("ManejadorCliente [" + logContext
                                                        + "]: Respuesta inesperada del worker " + workerId);
                                        return new Respuesta(EstadoOperacion.ERROR_COMUNICACION,
                                                        "Respuesta inesperada del worker " + workerId, null);
                                } catch (SocketTimeoutException e) {
                                        System.err.println("ManejadorCliente [" + logContext + "]: Timeout con worker "
                                                        + workerId + ". Desregistrando.");
                                        ServidorCentral.desregistrarWorker(workerId);
                                } catch (IOException e) {
                                        System.err.println("ManejadorCliente [" + logContext + "]: Error IO con worker "
                                                        + workerId + ": " + e.getMessage() + ". Desregistrando.");
                                        ServidorCentral.desregistrarWorker(workerId);
                                } catch (ClassNotFoundException e) {
                                        System.err.println("ManejadorCliente [" + logContext
                                                        + "]: Error ClassNotFound de worker " + workerId + ": "
                                                        + e.getMessage());
                                        return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                                                        "Error de formato en respuesta del worker.", null);
                                }
                        }
                }
                return new Respuesta(EstadoOperacion.ERROR_REINTENTAR_EN_OTRO_NODO, "No se pudo contactar workers para "
                                + tipoSubOperacion + " en partición " + idParticion, null);
        }

        private void replicarActualizacionSaldo(String idParticion, int idCuenta, double nuevoSaldo,
                        int idTransaccionGlobal, String workerPrimarioId) {
                List<String> idsWorkersConParticion = ServidorCentral.particionANodos.get(idParticion);
                if (idsWorkersConParticion == null)
                        return;

                System.out.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                + "]: Iniciando replicación de saldo para cuenta " + idCuenta + " en partición "
                                + idParticion + " (nuevo saldo: " + nuevoSaldo + ")");

                for (String workerIdReplica : new ArrayList<>(idsWorkersConParticion)) {
                        if (workerIdReplica.equals(workerPrimarioId)) {
                                continue; // No replicar al worker que ya hizo el cambio primario
                        }
                        InfoWorker infoWorkerReplica = ServidorCentral.workersActivos.get(workerIdReplica);
                        if (infoWorkerReplica != null && infoWorkerReplica.isActivo()) {
                                System.out.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                                + "]: Replicando saldo de cta " + idCuenta + " a worker réplica "
                                                + workerIdReplica);
                                try (Socket socketAlWorker = new Socket(infoWorkerReplica.getHost(),
                                                infoWorkerReplica.getPuertoTareas());
                                                ObjectOutputStream oosWorker = new ObjectOutputStream(
                                                                socketAlWorker.getOutputStream());
                                                ObjectInputStream oisWorker = new ObjectInputStream(
                                                                socketAlWorker.getInputStream())) {
                                        socketAlWorker.setSoTimeout(5000);
                                        Map<String, Object> paramsReplica = Map.of("ID_CUENTA", idCuenta, "NUEVO_SALDO",
                                                        nuevoSaldo, "ID_PARTICION", idParticion,
                                                        "ID_TRANSACCION_GLOBAL", idTransaccionGlobal);
                                        Solicitud solicitudReplica = new Solicitud(
                                                        TipoOperacion.ACTUALIZAR_SALDO_REPLICA, paramsReplica);
                                        oosWorker.writeObject(solicitudReplica);
                                        oosWorker.flush();

                                        Object ackObj = oisWorker.readObject();
                                        if (ackObj instanceof Respuesta) {
                                                Respuesta ackResp = (Respuesta) ackObj;
                                                if (ackResp.getEstado() == EstadoOperacion.REPLICA_ACTUALIZADA_OK) {
                                                        System.out.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                                                        + "]: Worker " + workerIdReplica
                                                                        + " confirmó replicación para cta " + idCuenta);
                                                } else {
                                                        System.err.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                                                        + "]: Worker " + workerIdReplica
                                                                        + " falló al replicar para cta " + idCuenta
                                                                        + ": " + ackResp.getMensaje());
                                                }
                                        } else {
                                                System.err.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                                                + "]: Respuesta de replicación inesperada de worker "
                                                                + workerIdReplica);
                                        }
                                } catch (Exception e) {
                                        System.err.println("ManejadorCliente [Tx:" + idTransaccionGlobal
                                                        + "]: Error al replicar saldo a worker " + workerIdReplica
                                                        + " para cta " + idCuenta + ": " + e.getMessage());
                                        // Considerar desregistrar o marcar como "necesita sincronización"
                                }
                        }
                }
        }

        private void enviarRespuesta(Respuesta respuesta) {
                if (!activo || oos == null || socketCliente.isClosed()) {
                        System.err.println(
                                        "ManejadorCliente: No se puede enviar respuesta, conexión inactiva o cerrada.");
                        return;
                }
                try {
                        oos.writeObject(respuesta);
                        oos.flush();
                        System.out.println("Respuesta enviada a [" + socketCliente.getInetAddress().getHostAddress()
                                        + "]: "
                                        + respuesta.getEstado() + " - " + respuesta.getMensaje());
                } catch (IOException e) {
                        System.err.println("ManejadorCliente: Error al enviar respuesta a "
                                        + socketCliente.getInetAddress().getHostAddress() + ": " + e.getMessage());
                        activo = false; // Marcar como inactivo si falla el envío
                        cerrarRecursos(); // Cerrar todo si no podemos enviar respuesta
                }
        }

        private void cerrarSocket() {
                try {
                        if (socketCliente != null && !socketCliente.isClosed()) {
                                socketCliente.close();
                        }
                } catch (IOException ex) {
                        // System.err.println("ManejadorCliente: Error al cerrar socket del cliente: " +
                        // ex.getMessage());
                }
        }

        private void cerrarRecursos() {
                if (!activo && (socketCliente == null || socketCliente.isClosed())) {
                        return;
                }
                activo = false;
                String address = (socketCliente != null && socketCliente.getInetAddress() != null)
                                ? socketCliente.getInetAddress().getHostAddress()
                                : "desconocido";
                System.out.println("ManejadorCliente: Cerrando conexión y recursos para " + address);
                try {
                        if (ois != null)
                                ois.close();
                } catch (IOException e) {
                        // System.err.println("ManejadorCliente: Error al cerrar ObjectInputStream: " +
                        // e.getMessage());
                }
                try {
                        if (oos != null)
                                oos.close();
                } catch (IOException e) {
                        // System.err.println("ManejadorCliente: Error al cerrar ObjectOutputStream: " +
                        // e.getMessage());
                }
                cerrarSocket();
        }
}
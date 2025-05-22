package workernode;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.EOFException;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import commons.Cliente;
import commons.Cuenta;
import commons.MensajeWorker;
import commons.Solicitud;
import commons.Respuesta;
import commons.EstadoOperacion;
import commons.TipoOperacion;

public class NodoTrabajador {
    public String workerId;
    private List<String> particionesAsignadasLocalmente;
    private String hostServidorCentral;
    private int puertoServidorCentral;
    private int puertoEscuchaTareas;
    private boolean registradoYDatosCargados = false;
    private String directorioBaseDatos;
    private String archivoLogTransaccionesLocal;

    private static final int MAX_TAREAS_CONCURRENTES = 10;

    public NodoTrabajador(String workerId, String hostServidorCentral, int puertoServidorCentral,
            int puertoEscuchaTareas, List<String> particionesSugeridasAlServidor) {
        this.workerId = workerId;
        this.hostServidorCentral = hostServidorCentral;
        this.puertoServidorCentral = puertoServidorCentral;
        this.puertoEscuchaTareas = puertoEscuchaTareas;
        this.particionesAsignadasLocalmente = new ArrayList<>();
        this.directorioBaseDatos = "data_" + workerId;
        this.archivoLogTransaccionesLocal = this.directorioBaseDatos + File.separator + "transacciones_locales.log";
        new File(this.directorioBaseDatos).mkdirs();
    }

    public void iniciar() {
        if (registrarYRecibirDatos()) {
            System.out.println("Worker [" + workerId + "]: Registrado y datos de partición recibidos. Directorio: "
                    + this.directorioBaseDatos + ". Iniciando escucha de tareas en puerto " + puertoEscuchaTareas);
            registradoYDatosCargados = true;
            escucharTareas();
        } else {
            System.err.println("Worker [" + workerId + "]: Proceso de registro y carga de datos fallido. Abortando.");
        }
    }

    private boolean registrarYRecibirDatos() {
        try (Socket socket = new Socket(hostServidorCentral, puertoServidorCentral);
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {
            System.out.println("Worker [" + workerId + "]: Intentando registrar con Servidor Central en "
                    + hostServidorCentral + ":" + puertoServidorCentral);
            MensajeWorker msgRegistro = new MensajeWorker(workerId, new ArrayList<>(), this.puertoEscuchaTareas);
            oos.writeObject(msgRegistro);
            oos.flush();
            System.out.println("Worker [" + workerId + "]: Mensaje de REGISTRO enviado (puerto tareas: "
                    + this.puertoEscuchaTareas + "). Esperando asignación de particiones...");

            Object respuestaMsgObj = ois.readObject();
            if (!(respuestaMsgObj instanceof MensajeWorker)) {
                System.err
                        .println("Worker [" + workerId + "]: Respuesta inesperada del servidor, no es MensajeWorker.");
                return false;
            }
            MensajeWorker msgAsignacion = (MensajeWorker) respuestaMsgObj;
            if (msgAsignacion.getTipo() == MensajeWorker.TipoMensaje.ASIGNACION_PARTICIONES_Y_DATOS) {
                System.out.println("Worker [" + workerId + "]: Recibida asignación de particiones y datos: "
                        + msgAsignacion.getListaParticiones());
                if (guardarDatosDeParticiones(msgAsignacion.getDatosPorParticion())) {
                    this.particionesAsignadasLocalmente.addAll(msgAsignacion.getListaParticiones());
                    MensajeWorker msgConfirmacionDatos = new MensajeWorker(
                            MensajeWorker.TipoMensaje.DATOS_RECIBIDOS_POR_WORKER, this.workerId,
                            "Datos de partición recibidos y guardados.");
                    oos.writeObject(msgConfirmacionDatos);
                    oos.flush();
                    System.out.println("Worker [" + workerId + "]: Confirmación de DATOS_RECIBIDOS enviada.");

                    Object msgFinalObj = ois.readObject();
                    if (msgFinalObj instanceof MensajeWorker) {
                        MensajeWorker msgFinalConfirmacion = (MensajeWorker) msgFinalObj;
                        if (msgFinalConfirmacion
                                .getTipo() == MensajeWorker.TipoMensaje.CONFIRMACION_REGISTRO_COMPLETO) {
                            System.out.println("Worker [" + workerId + "]: Registro completo confirmado por servidor: "
                                    + msgFinalConfirmacion.getMensajeTexto());
                            return true;
                        } else {
                            System.err.println("Worker [" + workerId
                                    + "]: Se esperaba CONFIRMACION_REGISTRO_COMPLETO, se recibió: "
                                    + msgFinalConfirmacion.getTipo());
                        }
                    } else {
                        System.err.println(
                                "Worker [" + workerId + "]: Respuesta final del servidor no es MensajeWorker.");
                    }
                } else {
                    System.err.println("Worker [" + workerId + "]: Error al guardar los datos de partición recibidos.");
                    oos.writeObject(new MensajeWorker(MensajeWorker.TipoMensaje.ERROR, this.workerId,
                            "Error al guardar datos de partición"));
                    oos.flush();
                }
            } else if (msgAsignacion.getTipo() == MensajeWorker.TipoMensaje.ERROR) {
                System.err.println("Worker [" + workerId + "]: Servidor envió error durante asignación: "
                        + msgAsignacion.getMensajeTexto());
            } else {
                System.err.println(
                        "Worker [" + workerId + "]: Respuesta inesperada del servidor, se esperaba ASIGNACION_DATOS: "
                                + msgAsignacion.getTipo());
            }

        } catch (UnknownHostException e) {
            System.err.println("Worker [" + workerId + "]: Host del servidor central no encontrado ("
                    + hostServidorCentral + "): " + e.getMessage());
        } catch (IOException e) {
            System.err.println("Worker [" + workerId + "]: Error de IO al registrar/recibir datos: " + e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(
                    "Worker [" + workerId + "]: Error ClassNotFound al recibir del servidor: " + e.getMessage());
        }
        return false;
    }

    private boolean guardarDatosDeParticiones(Map<String, List<? extends Serializable>> datosPorParticion) {
        if (datosPorParticion == null)
            return false;
        System.out.println("Worker [" + workerId + "]: Guardando datos de particiones recibidas...");
        for (Map.Entry<String, List<? extends Serializable>> entry : datosPorParticion.entrySet()) {
            String idParticion = entry.getKey();
            List<? extends Serializable> listaDatos = entry.getValue();
            String nombreArchivo = directorioBaseDatos + File.separator + idParticion + ".txt";

            try (BufferedWriter bw = new BufferedWriter(new FileWriter(nombreArchivo, false))) {
                if (idParticion.startsWith("CUENTA_P")) {
                    bw.write("ID_CUENTA|ID_CLIENTE|SALDO|TIPO_CUENTA\n");
                    for (Serializable dato : listaDatos) {
                        if (dato instanceof Cuenta) {
                            Cuenta c = (Cuenta) dato;
                            bw.write(String.format("%d|%d|%.2f|%s\n", c.getIdCuenta(), c.getIdCliente(), c.getSaldo(),
                                    c.getTipoCuenta()).replace(',', '.'));
                        }
                    }
                } else if (idParticion.startsWith("CLIENTE_P")) {
                    bw.write("ID_CLIENTE|NOMBRE|EMAIL|TELEFONO\n");
                    for (Serializable dato : listaDatos) {
                        if (dato instanceof Cliente) {
                            Cliente c = (Cliente) dato;
                            bw.write(String.format("%d|%s|%s|%s\n", c.getIdCliente(), c.getNombre(), c.getEmail(),
                                    c.getTelefono()));
                        }
                    }
                }
                System.out.println("Worker [" + workerId + "]: Datos para partición " + idParticion + " guardados en "
                        + nombreArchivo);
            } catch (IOException e) {
                System.err.println("Worker [" + workerId + "]: Error al escribir archivo de partición " + nombreArchivo
                        + ": " + e.getMessage());
                return false;
            }
        }
        return true;
    }

    private void escucharTareas() {
        ExecutorService poolTareas = Executors.newFixedThreadPool(MAX_TAREAS_CONCURRENTES);
        try (ServerSocket serverSocketTareas = new ServerSocket(puertoEscuchaTareas)) {
            System.out.println("Worker [" + workerId + "]: Escuchando tareas del Servidor Central en puerto "
                    + puertoEscuchaTareas);
            while (registradoYDatosCargados) {
                try {
                    Socket socketTarea = serverSocketTareas.accept();
                    System.out.println("Worker [" + workerId + "]: Solicitud de tarea recibida de "
                            + socketTarea.getInetAddress());
                    poolTareas.execute(new ManejadorTareaWorker(socketTarea, this));
                } catch (IOException e) {
                    if (!registradoYDatosCargados) {
                        System.out.println("Worker [" + workerId
                                + "]: Dejando de escuchar tareas porque ya no está registrado/cargado.");
                        break;
                    }
                    System.err.println(
                            "Worker [" + workerId + "]: Error al aceptar conexión de tarea: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Worker [" + workerId + "]: Error CRÍTICO al iniciar ServerSocket para tareas en puerto "
                    + puertoEscuchaTareas + ": " + e.getMessage());
        } finally {
            poolTareas.shutdown();
            System.out.println("Worker [" + workerId + "]: Servicio de escucha de tareas terminado.");
        }
    }

    private synchronized Double leerSaldoDeArchivo(String idParticion, int idCuentaBuscada) {
        String nombreArchivo = directorioBaseDatos + File.separator + idParticion + ".txt";
        try (BufferedReader br = new BufferedReader(new FileReader(nombreArchivo))) {
            String linea = br.readLine(); // Saltar cabecera
            while ((linea = br.readLine()) != null) {
                String[] partes = linea.split("\\|");
                if (partes.length >= 3) {
                    try {
                        int idCuentaArchivo = Integer.parseInt(partes[0].trim());
                        if (idCuentaArchivo == idCuentaBuscada) {
                            return Double.parseDouble(partes[2].trim().replace(',', '.'));
                        }
                    } catch (NumberFormatException e) {
                        System.err.println("Worker [" + workerId + "]: Error al parsear línea (saldo) en "
                                + nombreArchivo + ": " + linea + " - " + e.getMessage());
                    }
                }
            }
        } catch (IOException e) {
            System.err.println("Worker [" + workerId + "]: Error al leer archivo de partición " + nombreArchivo
                    + " para saldo: " + e.getMessage());
        }
        return null;
    }

    private synchronized boolean actualizarSaldosEnArchivo(String idParticion, int idCuentaOrigen,
            double nuevoSaldoOrigen, int idCuentaDestino, double nuevoSaldoDestino) {
        String nombreArchivo = directorioBaseDatos + File.separator + idParticion + ".txt";
        String nombreArchivoTemp = nombreArchivo + ".tmp";
        List<String> lineas = new ArrayList<>();
        boolean origenActualizado = false;
        boolean destinoActualizado = false;

        try (BufferedReader br = new BufferedReader(new FileReader(nombreArchivo))) {
            String linea;
            while ((linea = br.readLine()) != null) {
                lineas.add(linea);
            }
        } catch (IOException e) {
            System.err.println("Worker [" + workerId + "]: Error al leer archivo para actualizar saldos: "
                    + nombreArchivo + " - " + e.getMessage());
            return false;
        }

        try (BufferedWriter bw = new BufferedWriter(new FileWriter(nombreArchivoTemp))) {
            for (int i = 0; i < lineas.size(); i++) {
                String linea = lineas.get(i);
                if (i == 0) { // Cabecera
                    bw.write(linea + "\n");
                    continue;
                }
                String[] partes = linea.split("\\|");
                if (partes.length >= 4) {
                    try {
                        int idCuentaActual = Integer.parseInt(partes[0].trim());
                        if (idCuentaActual == idCuentaOrigen) {
                            bw.write(String.format("%d|%s|%.2f|%s\n", idCuentaActual, partes[1].trim(),
                                    nuevoSaldoOrigen, partes[3].trim()).replace(',', '.'));
                            origenActualizado = true;
                        } else if (idCuentaActual == idCuentaDestino) {
                            bw.write(String.format("%d|%s|%.2f|%s\n", idCuentaActual, partes[1].trim(),
                                    nuevoSaldoDestino, partes[3].trim()).replace(',', '.'));
                            destinoActualizado = true;
                        } else {
                            bw.write(linea + "\n");
                        }
                    } catch (NumberFormatException e) {
                        System.err.println(
                                "Worker [" + workerId + "]: Error parseando ID en línea para actualizar: " + linea);
                        bw.write(linea + "\n"); // Escribir original si hay error de parseo
                    }
                } else {
                    bw.write(linea + "\n"); // Línea mal formada, escribirla tal cual
                }
            }
        } catch (IOException e) {
            System.err.println("Worker [" + workerId + "]: Error al escribir archivo temporal: " + nombreArchivoTemp
                    + " - " + e.getMessage());
            return false;
        }

        if (!origenActualizado && idCuentaOrigen != -1) { // idCuentaOrigen -1 si es solo depósito/retiro sin origen
            System.err.println(
                    "Worker [" + workerId + "]: Cuenta origen " + idCuentaOrigen + " no encontrada para actualizar.");
            return false; // O manejar de otra forma, ej. si la operación solo involucra destino
        }
        if (!destinoActualizado && idCuentaDestino != -1) {
            System.err.println(
                    "Worker [" + workerId + "]: Cuenta destino " + idCuentaDestino + " no encontrada para actualizar.");
            return false;
        }

        File original = new File(nombreArchivo);
        File temp = new File(nombreArchivoTemp);
        if (original.delete()) {
            if (!temp.renameTo(original)) {
                System.err.println("Worker [" + workerId + "]: Error al renombrar archivo temporal a original.");
                return false;
            }
        } else {
            System.err.println("Worker [" + workerId + "]: Error al borrar archivo original.");
            return false;
        }
        return true;
    }

    private synchronized void registrarTransaccionLocal(int idTransaccionGlobal, int idCuentaOrigen,
            int idCuentaDestino, double monto, String estadoDetalle) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(this.archivoLogTransaccionesLocal, true))) {
            long timestamp = System.currentTimeMillis();
            bw.write(String.format("TxGlobal:%d|%d|%d|%.2f|%s|%s\n",
                    idTransaccionGlobal, idCuentaOrigen, idCuentaDestino, monto, new Date(timestamp).toString(),
                    estadoDetalle).replace(',', '.'));
        } catch (IOException e) {
            System.err.println(
                    "Worker [" + workerId + "]: Error al escribir en log de transacciones local: " + e.getMessage());
        }
    }

    private synchronized boolean actualizarSaldoUnicaCuentaEnArchivo(String idParticion, int idCuentaAActualizar,
            double nuevoSaldo, String operacionDesc) {
        String nombreArchivo = directorioBaseDatos + File.separator + idParticion + ".txt";
        File archivoOriginal = new File(nombreArchivo);
        File archivoTemporal = new File(nombreArchivo + ".tmp");
        List<String> lineas = new ArrayList<>();
        boolean cuentaActualizada = false;

        if (!archivoOriginal.exists()) {
            System.err.println("Worker [" + workerId + "]: Archivo de partición no existe para " + operacionDesc + ": "
                    + nombreArchivo);
            return false;
        }
        try (BufferedReader br = new BufferedReader(new FileReader(archivoOriginal))) {
            String linea;
            while ((linea = br.readLine()) != null) {
                lineas.add(linea);
            }
        } catch (IOException e) {
            System.err.println("Worker [" + workerId + "]: Error al leer archivo para " + operacionDesc + ": "
                    + nombreArchivo + " - " + e.getMessage());
            return false;
        }
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(archivoTemporal))) {
            for (int i = 0; i < lineas.size(); i++) {
                String linea = lineas.get(i);
                if (i == 0) {
                    bw.write(linea + "\n");
                    continue;
                }
                String[] partes = linea.split("\\|");
                if (partes.length >= 4) {
                    try {
                        int idCuentaActualArchivo = Integer.parseInt(partes[0].trim());
                        if (idCuentaActualArchivo == idCuentaAActualizar) {
                            bw.write(String.format("%d|%s|%.2f|%s\n", idCuentaActualArchivo, partes[1].trim(),
                                    nuevoSaldo, partes[3].trim()).replace(',', '.'));
                            cuentaActualizada = true;
                        } else {
                            bw.write(linea + "\n");
                        }
                    } catch (NumberFormatException e) {
                        System.err.println("Worker [" + workerId + "]: Error parseando ID en " + nombreArchivo
                                + " linea: " + linea);
                        bw.write(linea + "\n");
                    }
                } else {
                    bw.write(linea + "\n");
                }
            }
        } catch (IOException e) {
            System.err.println("Worker [" + workerId + "]: Error al escribir archivo temporal para " + operacionDesc
                    + ": " + e.getMessage());
            archivoTemporal.delete();
            return false;
        }
        if (!cuentaActualizada) {
            System.err.println("Worker [" + workerId + "]: Cuenta " + idCuentaAActualizar + " no encontrada para "
                    + operacionDesc + " en " + idParticion + ".");
            archivoTemporal.delete();
            return false;
        }
        if (!archivoOriginal.delete()) {
            System.err
                    .println("Worker [" + workerId + "]: Error al borrar archivo original para " + operacionDesc + ".");
            archivoTemporal.delete();
            return false;
        }
        if (!archivoTemporal.renameTo(archivoOriginal)) {
            System.err.println(
                    "Worker [" + workerId + "]: Error al renombrar archivo temporal para " + operacionDesc + ".");
            return false;
        }
        System.out.println("Worker [" + workerId + "]: Saldo actualizado para cta " + idCuentaAActualizar + " en "
                + idParticion + " (" + operacionDesc + "). Nuevo Saldo: " + nuevoSaldo);
        return true;
    }

    public Respuesta procesarSolicitud(Solicitud solicitud) {
        System.out.println("Worker [" + workerId + "]: Procesando " + solicitud.getTipoOperacion() + " params: "
                + solicitud.getParametros());
        Integer idTransaccionGlobal = (Integer) solicitud.getParametros().getOrDefault("ID_TRANSACCION_GLOBAL", -1);
        String idParticionSolicitada = (String) solicitud.getParametros().get("ID_PARTICION");

        // Verificar si el worker maneja la partición (excepto para operaciones que no
        // la requieran directamente)
        if (idParticionSolicitada != null && !particionesAsignadasLocalmente.contains(idParticionSolicitada)) {
            String errorMsg = "Worker no maneja la partición " + idParticionSolicitada + " para operación "
                    + solicitud.getTipoOperacion();
            System.err.println("Worker [" + workerId + "]: " + errorMsg);
            return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR, errorMsg, null);
        }

        switch (solicitud.getTipoOperacion()) {
            case CONSULTAR_SALDO:
                Integer idCuentaConsulta = (Integer) solicitud.getParametros().get("ID_CUENTA");
                if (idCuentaConsulta == null || idParticionSolicitada == null)
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR, "Params incompletos CONSULTAR_SALDO",
                            null);
                Double saldo = leerSaldoDeArchivo(idParticionSolicitada, idCuentaConsulta);
                if (saldo != null) {
                    return new Respuesta(EstadoOperacion.EXITO, "Saldo: " + saldo, saldo);
                } else {
                    return new Respuesta(EstadoOperacion.ERROR_CUENTA_ORIGEN_NO_EXISTE,
                            "Cta " + idCuentaConsulta + " no en part " + idParticionSolicitada, null);
                }

            case TRANSFERIR_FONDOS: // Caso A: Misma partición, worker realiza toda la lógica
                Integer idCtaOrigen = (Integer) solicitud.getParametros().get("ID_CUENTA_ORIGEN");
                Integer idCtaDestino = (Integer) solicitud.getParametros().get("ID_CUENTA_DESTINO");
                Double monto = (Double) solicitud.getParametros().get("MONTO");
                if (idCtaOrigen == null || idCtaDestino == null || monto == null || idParticionSolicitada == null)
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR, "Params incompletos TRANSFERIR_FONDOS",
                            null);

                Double saldoO = leerSaldoDeArchivo(idParticionSolicitada, idCtaOrigen);
                if (saldoO == null)
                    return new Respuesta(EstadoOperacion.ERROR_CUENTA_ORIGEN_NO_EXISTE,
                            "CtaOrigen " + idCtaOrigen + " no existe", null);
                if (saldoO < monto) {
                    registrarTransaccionLocal(idTransaccionGlobal, idCtaOrigen, idCtaDestino, monto,
                            "RECHAZADA_SALDO_INSUF_W" + workerId);
                    return new Respuesta(EstadoOperacion.ERROR_SALDO_INSUFICIENTE,
                            "Saldo insuficiente CtaOrigen " + idCtaOrigen, saldoO);
                }
                Double saldoD = leerSaldoDeArchivo(idParticionSolicitada, idCtaDestino);
                if (saldoD == null) {
                    registrarTransaccionLocal(idTransaccionGlobal, idCtaOrigen, idCtaDestino, monto,
                            "FALLIDA_DESTINO_NO_EXISTE_W" + workerId);
                    return new Respuesta(EstadoOperacion.ERROR_CUENTA_DESTINO_NO_EXISTE,
                            "CtaDestino " + idCtaDestino + " no existe", null);
                }
                double nSaldoO = saldoO - monto;
                double nSaldoD = saldoD + monto;
                if (actualizarSaldosEnArchivo(idParticionSolicitada, idCtaOrigen, nSaldoO, idCtaDestino, nSaldoD)) {
                    registrarTransaccionLocal(idTransaccionGlobal, idCtaOrigen, idCtaDestino, monto,
                            "EXITO_INTRA_PARTICION_W" + workerId);
                    return new Respuesta(EstadoOperacion.EXITO, "Transferencia intra-partición OK por " + workerId,
                            Map.of("nuevoSaldoOrigen", nSaldoO, "nuevoSaldoDestino", nSaldoD, "workerIdProcesador",
                                    workerId));
                } else {
                    registrarTransaccionLocal(idTransaccionGlobal, idCtaOrigen, idCtaDestino, monto,
                            "FALLIDA_ESCRITURA_W" + workerId);
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                            "Error escritura en " + idParticionSolicitada, null);
                }

            case PREPARAR_DEBITO:
                idCtaOrigen = (Integer) solicitud.getParametros().get("ID_CUENTA_ORIGEN");
                monto = (Double) solicitud.getParametros().get("MONTO");
                if (idCtaOrigen == null || monto == null || idParticionSolicitada == null)
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR, "Params incompletos PREPARAR_DEBITO",
                            null);
                saldoO = leerSaldoDeArchivo(idParticionSolicitada, idCtaOrigen);
                if (saldoO == null)
                    return new Respuesta(EstadoOperacion.ERROR_CUENTA_ORIGEN_NO_EXISTE,
                            "CtaOrigen " + idCtaOrigen + " no existe", null);
                if (saldoO < monto)
                    return new Respuesta(EstadoOperacion.ERROR_SALDO_INSUFICIENTE, "Saldo insuficiente " + idCtaOrigen,
                            saldoO);
                registrarTransaccionLocal(idTransaccionGlobal, idCtaOrigen, -1, monto,
                        "PREPARAR_DEBITO_OK_W" + workerId);
                return new Respuesta(EstadoOperacion.DEBITO_PREPARADO_OK, "Débito preparado por " + workerId,
                        Map.of("saldoActualOrigen", saldoO, "workerIdProcesador", workerId));

            case APLICAR_CREDITO:
                idCtaDestino = (Integer) solicitud.getParametros().get("ID_CUENTA_DESTINO");
                monto = (Double) solicitud.getParametros().get("MONTO");
                if (idCtaDestino == null || monto == null || idParticionSolicitada == null)
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR, "Params incompletos APLICAR_CREDITO",
                            null);
                saldoD = leerSaldoDeArchivo(idParticionSolicitada, idCtaDestino);
                if (saldoD == null)
                    return new Respuesta(EstadoOperacion.ERROR_CUENTA_DESTINO_NO_EXISTE,
                            "CtaDestino " + idCtaDestino + " no existe", null);
                nSaldoD = saldoD + monto;
                if (actualizarSaldoUnicaCuentaEnArchivo(idParticionSolicitada, idCtaDestino, nSaldoD,
                        "APLICAR_CREDITO")) {
                    registrarTransaccionLocal(idTransaccionGlobal, -1, idCtaDestino, monto,
                            "APLICAR_CREDITO_OK_W" + workerId);
                    return new Respuesta(EstadoOperacion.CREDITO_APLICADO_OK, "Crédito aplicado por " + workerId,
                            Map.of("nuevoSaldoDestino", nSaldoD, "workerIdProcesador", workerId));
                } else {
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR, "Error escritura APLICAR_CREDITO",
                            null);
                }

            case CONFIRMAR_DEBITO:
                idCtaOrigen = (Integer) solicitud.getParametros().get("ID_CUENTA_ORIGEN");
                monto = (Double) solicitud.getParametros().get("MONTO");
                if (idCtaOrigen == null || monto == null || idParticionSolicitada == null)
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR, "Params incompletos CONFIRMAR_DEBITO",
                            null);
                saldoO = leerSaldoDeArchivo(idParticionSolicitada, idCtaOrigen);
                if (saldoO == null)
                    return new Respuesta(EstadoOperacion.ERROR_CUENTA_ORIGEN_NO_EXISTE,
                            "CtaOrigen " + idCtaOrigen + " no existe (CONFIRMAR_DEBITO)", null);
                // Se asume que el saldo fue suficiente porque PREPARAR_DEBITO tuvo éxito.
                nSaldoO = saldoO - monto;
                if (actualizarSaldoUnicaCuentaEnArchivo(idParticionSolicitada, idCtaOrigen, nSaldoO,
                        "CONFIRMAR_DEBITO")) {
                    registrarTransaccionLocal(idTransaccionGlobal, idCtaOrigen, -1, monto,
                            "CONFIRMAR_DEBITO_OK_W" + workerId);
                    return new Respuesta(EstadoOperacion.DEBITO_CONFIRMADO_OK, "Débito confirmado por " + workerId,
                            Map.of("nuevoSaldoOrigen", nSaldoO, "workerIdProcesador", workerId));
                } else {
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR, "Error escritura CONFIRMAR_DEBITO",
                            null);
                }

            case REVERTIR_DEBITO:
                idCtaOrigen = (Integer) solicitud.getParametros().get("ID_CUENTA_ORIGEN");
                monto = (Double) solicitud.getParametros().get("MONTO");
                if (idCtaOrigen == null || monto == null || idParticionSolicitada == null)
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR, "Params incompletos REVERTIR_DEBITO",
                            null);
                saldoO = leerSaldoDeArchivo(idParticionSolicitada, idCtaOrigen);
                if (saldoO == null)
                    return new Respuesta(EstadoOperacion.ERROR_CUENTA_ORIGEN_NO_EXISTE,
                            "CtaOrigen " + idCtaOrigen + " no existe (REVERTIR_DEBITO)", null);
                nSaldoO = saldoO + monto; // Sumar de vuelta
                if (actualizarSaldoUnicaCuentaEnArchivo(idParticionSolicitada, idCtaOrigen, nSaldoO,
                        "REVERTIR_DEBITO")) {
                    registrarTransaccionLocal(idTransaccionGlobal, idCtaOrigen, -1, monto,
                            "REVERTIR_DEBITO_OK_W" + workerId);
                    return new Respuesta(EstadoOperacion.DEBITO_REVERTIDO_OK, "Débito revertido por " + workerId,
                            Map.of("nuevoSaldoOrigen", nSaldoO, "workerIdProcesador", workerId));
                } else {
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR, "Error escritura REVERTIR_DEBITO",
                            null);
                }

            case ACTUALIZAR_SALDO_REPLICA:
                Integer idCuentaReplica = (Integer) solicitud.getParametros().get("ID_CUENTA");
                Double nuevoSaldoReplica = (Double) solicitud.getParametros().get("NUEVO_SALDO");
                if (idCuentaReplica == null || nuevoSaldoReplica == null || idParticionSolicitada == null)
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                            "Params incompletos ACTUALIZAR_SALDO_REPLICA", null);
                if (actualizarSaldoUnicaCuentaEnArchivo(idParticionSolicitada, idCuentaReplica, nuevoSaldoReplica,
                        "ACTUALIZAR_SALDO_REPLICA")) {
                    System.out.println(
                            "Worker [" + workerId + "] [Tx:" + idTransaccionGlobal + "]: Réplica actualizada para cta "
                                    + idCuentaReplica + " en part " + idParticionSolicitada);
                    return new Respuesta(EstadoOperacion.REPLICA_ACTUALIZADA_OK, "Réplica actualizada por " + workerId,
                            null);
                } else {
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                            "Error escritura ACTUALIZAR_SALDO_REPLICA", null);
                }

            case CALCULAR_SALDO_PARTICION:
                if (idParticionSolicitada == null) {
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                            "ID_PARTICION no proporcionado para CALCULAR_SALDO_PARTICION.", null);
                }
                if (!particionesAsignadasLocalmente.contains(idParticionSolicitada)) {
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                            "Worker no maneja la partición " + idParticionSolicitada + " para arqueo.", null);
                }
                double sumaSaldosParticion = 0.0;
                String nombreArchivo = directorioBaseDatos + File.separator + idParticionSolicitada + ".txt";
                System.out.println("Worker [" + workerId + "]: Calculando saldo total para partición "
                        + idParticionSolicitada + " desde archivo " + nombreArchivo);
                try (BufferedReader br = new BufferedReader(new FileReader(nombreArchivo))) {
                    String linea = br.readLine(); // Saltar cabecera
                    while ((linea = br.readLine()) != null) {
                        String[] partes = linea.split("\\|");
                        if (partes.length >= 3) { // ID_CUENTA|ID_CLIENTE|SALDO|...
                            try {
                                double saldoCuenta = Double.parseDouble(partes[2].trim().replace(',', '.'));
                                sumaSaldosParticion += saldoCuenta;
                            } catch (NumberFormatException e) {
                                System.err
                                        .println("Worker [" + workerId + "]: Error al parsear saldo en línea (arqueo): "
                                                + linea + " - " + e.getMessage());
                            }
                        }
                    }
                    System.out.println("Worker [" + workerId + "]: Suma de saldos para partición "
                            + idParticionSolicitada + " es: " + sumaSaldosParticion);
                    return new Respuesta(EstadoOperacion.EXITO, "Suma de saldos de partición " + idParticionSolicitada
                            + " calculada por worker " + workerId, sumaSaldosParticion);
                } catch (IOException e) {
                    System.err.println("Worker [" + workerId + "]: Error al leer archivo para CALCULAR_SALDO_PARTICION "
                            + nombreArchivo + ": " + e.getMessage());
                    return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                            "Error al leer archivo de partición para arqueo.", null);
                }

            default:
                return new Respuesta(EstadoOperacion.ERROR_GENERAL_SERVIDOR,
                        "Operación no soportada [" + solicitud.getTipoOperacion() + "].", null);
        }
    }

    public static void main(String[] args) {
        if (args.length < 4) {
            System.err.println(
                    "Uso: java NodoTrabajador <workerId> <hostServidorCentral> <puertoServidorCentral> <puertoEscuchaTareas> [particion1,particion2,...]");
            System.err.println("Ejemplo: java NodoTrabajador worker1 localhost 12346 12350");
            return;
        }

        String workerId = args[0];
        String hostServidor = args[1];
        int puertoServidor = Integer.parseInt(args[2]);
        int puertoTareas = Integer.parseInt(args[3]);
        List<String> particionesSugeridas = new ArrayList<>();

        NodoTrabajador worker = new NodoTrabajador(workerId, hostServidor, puertoServidor, puertoTareas,
                particionesSugeridas);
        worker.iniciar();
    }
}
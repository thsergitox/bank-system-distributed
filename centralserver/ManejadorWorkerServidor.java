package centralserver;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import commons.MensajeWorker;
import commons.InfoWorker;
import java.io.Serializable;

// Asumiendo que esta clase está en el mismo 'paquete por defecto' que ServidorCentral

public class ManejadorWorkerServidor implements Runnable {
    private Socket socketWorker;
    private ObjectOutputStream oos;
    private ObjectInputStream ois;
    private String workerId;
    private ServidorCentral servidorCentral; // Aunque los métodos de registro sean static, puede ser útil para otras
                                             // cosas
    private boolean activo;
    private InfoWorker infoEsteWorker; // Para mantener la info del worker que este hilo maneja

    public ManejadorWorkerServidor(Socket socketWorker, ServidorCentral servidorCentralInstance) {
        this.socketWorker = socketWorker;
        this.servidorCentral = servidorCentralInstance; // Guardar referencia (aunque usemos métodos estáticos de
                                                        // ServidorCentral para registro)
        this.activo = true;
        try {
            this.oos = new ObjectOutputStream(socketWorker.getOutputStream());
            this.ois = new ObjectInputStream(socketWorker.getInputStream());
            System.out.println("ManejadorWorker: Streams creados para worker " + socketWorker.getInetAddress());
        } catch (IOException e) {
            System.err.println("ManejadorWorker: Error al crear streams para worker " + socketWorker.getInetAddress()
                    + ": " + e.getMessage());
            activo = false;
            cerrarSocket();
        }
    }

    @Override
    public void run() {
        if (!activo) {
            return;
        }
        String remoteAddress = socketWorker.getInetAddress().getHostAddress();
        System.out.println("ManejadorWorker: Hilo iniciado para worker: " + remoteAddress);

        try {
            // 1. Recibir mensaje de REGISTRO
            Object msgObj = ois.readObject();
            if (!(msgObj instanceof MensajeWorker)) {
                System.err.println(
                        "ManejadorWorker [" + remoteAddress + "]: Mensaje inicial no es de tipo MensajeWorker.");
                cerrarRecursos();
                return;
            }
            MensajeWorker msgRegistro = (MensajeWorker) msgObj;
            if (msgRegistro.getTipo() != MensajeWorker.TipoMensaje.REGISTRO) {
                System.err.println("ManejadorWorker [" + remoteAddress
                        + "]: Se esperaba mensaje de REGISTRO pero se recibió: " + msgRegistro.getTipo());
                cerrarRecursos();
                return;
            }

            this.workerId = msgRegistro.getWorkerId();
            int puertoTareasWorker = msgRegistro.getPuertoTareasWorker();
            System.out.println("ManejadorWorker: Solicitud de REGISTRO recibida de worker " + this.workerId
                    + " en puerto de tareas " + puertoTareasWorker);

            // 2. Lógica de asignación de particiones (simplificada)
            List<String> particionesAsignadasAlWorker = new ArrayList<>();
            Map<String, List<? extends Serializable>> datosParaWorker = new HashMap<>();

            // Lógica de asignación (ejemplo: asignar la siguiente partición disponible con
            // menos de N réplicas)
            // Esto debe ser sincronizado o usar estructuras Concurrentes de forma segura en
            // ServidorCentral
            ServidorCentral.asignarParticionesAWorker(this.workerId, particionesAsignadasAlWorker, datosParaWorker);

            if (particionesAsignadasAlWorker.isEmpty()) {
                System.out.println(
                        "ManejadorWorker: No hay particiones disponibles para asignar al worker " + this.workerId);
                oos.writeObject(new MensajeWorker(MensajeWorker.TipoMensaje.ERROR, "ServidorCentral",
                        "No hay particiones para asignar."));
                oos.flush();
                cerrarRecursos();
                return;
            }

            // 3. Enviar ASIGNACION_DATOS_PARTICIONES
            MensajeWorker msgAsignacion = new MensajeWorker(datosParaWorker, particionesAsignadasAlWorker,
                    "Particiones asignadas.");
            oos.writeObject(msgAsignacion);
            oos.flush();
            System.out.println("ManejadorWorker: Enviados datos de particiones " + particionesAsignadasAlWorker
                    + " a worker " + this.workerId);

            // 4. Esperar DATOS_RECIBIDOS_POR_WORKER
            msgObj = ois.readObject();
            if (!(msgObj instanceof MensajeWorker)) {
                System.err.println("ManejadorWorker [" + this.workerId
                        + "]: Mensaje post-asignación no es de tipo MensajeWorker.");
                cerrarRecursos(); // Podríamos intentar desregistrar las particiones si ya se asignaron en el mapa
                                  // global
                return;
            }
            MensajeWorker msgConfirmacionDatos = (MensajeWorker) msgObj;

            if (msgConfirmacionDatos.getTipo() == MensajeWorker.TipoMensaje.DATOS_RECIBIDOS_POR_WORKER) {
                System.out.println("ManejadorWorker: Worker " + this.workerId + " confirmó recepción de datos: "
                        + msgConfirmacionDatos.getMensajeTexto());

                // 5. Registrar formalmente al worker y sus particiones
                this.infoEsteWorker = new InfoWorker(this.workerId, remoteAddress, puertoTareasWorker,
                        particionesAsignadasAlWorker, this.socketWorker);
                ServidorCentral.registrarActualizarWorker(this.infoEsteWorker, particionesAsignadasAlWorker);

                // 6. Enviar CONFIRMACION_REGISTRO_COMPLETO
                oos.writeObject(new MensajeWorker(this.workerId, "Registro completo y particiones asignadas.", true));
                oos.flush();
                System.out.println(
                        "ManejadorWorker: Confirmación de registro completo enviada a worker " + this.workerId);
                // El hilo podría quedar para heartbeats o terminar si la conexión inicial es
                // solo para registro.
                // Por ahora, lo mantenemos activo para un posible heartbeat futuro.
                // while(activo) { Thread.sleep(10000); /* heartbeat simulado */ }

            } else {
                System.err.println("ManejadorWorker [" + this.workerId
                        + "]: Se esperaba DATOS_RECIBIDOS_POR_WORKER pero fue " + msgConfirmacionDatos.getTipo());
                // Considerar limpiar las asignaciones de particionANodos si ya se hicieron
                ServidorCentral.removerParticionesDeWorker(this.workerId, particionesAsignadasAlWorker);
                cerrarRecursos();
                return;
            }

        } catch (EOFException e) {
            System.out.println(
                    "ManejadorWorker: Worker " + (workerId != null ? workerId : remoteAddress) + " cerró la conexión.");
            if (this.infoEsteWorker != null)
                ServidorCentral.desregistrarWorker(this.workerId);
        } catch (IOException e) {
            if (activo) {
                System.err.println("ManejadorWorker: Error de IO con worker "
                        + (workerId != null ? workerId : remoteAddress) + ": " + e.getMessage());
                if (this.infoEsteWorker != null)
                    ServidorCentral.desregistrarWorker(this.workerId);
            }
        } catch (ClassNotFoundException e) {
            System.err.println("ManejadorWorker: Error ClassNotFound con worker "
                    + (workerId != null ? workerId : remoteAddress) + ": " + e.getMessage());
            if (this.infoEsteWorker != null)
                ServidorCentral.desregistrarWorker(this.workerId);
        } finally {
            // Si no se registró infoEsteWorker (ej. fallo antes de confirmación de datos) y
            // workerId existe,
            // puede que necesitemos limpiar particionANodos si se asignaron algunas
            // particiones temporalmente.
            // La lógica de desregistrarWorker ya debería encargarse si infoEsteWorker fue
            // registrado.
            if (this.infoEsteWorker == null && this.workerId != null) {
                // System.out.println("Limpieza adicional para worker " + this.workerId + " si
                // es necesario.");
                // ServidorCentral.removerParticionesDeWorker(this.workerId, ...); //
                // Necesitaría la lista de particiones que se intentó asignar
            }
            cerrarRecursos();
        }
    }

    private void cerrarSocket() {
        try {
            if (socketWorker != null && !socketWorker.isClosed()) {
                socketWorker.close();
            }
        } catch (IOException ex) {
            /* Silenciado */ }
    }

    private void cerrarRecursos() {
        if (!activo && (this.workerId == null || (socketWorker != null && socketWorker.isClosed()))) {
            return;
        }
        activo = false;
        String address = (socketWorker != null && socketWorker.getInetAddress() != null)
                ? socketWorker.getInetAddress().getHostAddress()
                : "desconocido";
        System.out.println(
                "ManejadorWorker: Cerrando conexión y recursos para worker " + (workerId != null ? workerId : address));
        try {
            if (ois != null)
                ois.close();
        } catch (IOException e) {
            /* Silenciado */ }
        try {
            if (oos != null)
                oos.close();
        } catch (IOException e) {
            /* Silenciado */ }
        cerrarSocket();
    }
}
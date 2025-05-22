package workernode;

import java.io.EOFException;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import commons.Solicitud;
import commons.Respuesta;
import commons.EstadoOperacion;

public class ManejadorTareaWorker implements Runnable {
    private Socket socketTareaServidor; // Socket conectado al ServidorCentral para esta tarea específica
    private NodoTrabajador nodoTrabajador; // Referencia al worker que procesará la tarea
    private ObjectOutputStream oos;
    private ObjectInputStream ois;
    private boolean activo;

    public ManejadorTareaWorker(Socket socketTareaServidor, NodoTrabajador nodoTrabajador) {
        this.socketTareaServidor = socketTareaServidor;
        this.nodoTrabajador = nodoTrabajador;
        this.activo = true;
        try {
            // Orden importante: OOS primero, luego OIS
            this.oos = new ObjectOutputStream(socketTareaServidor.getOutputStream());
            this.ois = new ObjectInputStream(socketTareaServidor.getInputStream());
            System.out.println("ManejadorTareaWorker [" + nodoTrabajador.workerId
                    + "]: Streams creados para tarea desde " + socketTareaServidor.getInetAddress());
        } catch (IOException e) {
            System.err.println("ManejadorTareaWorker [" + nodoTrabajador.workerId
                    + "]: Error al crear streams para tarea: " + e.getMessage());
            activo = false;
            cerrarSocket();
        }
    }

    @Override
    public void run() {
        if (!activo) {
            return;
        }
        System.out.println("ManejadorTareaWorker [" + nodoTrabajador.workerId
                + "]: Hilo iniciado para manejar tarea de " + socketTareaServidor.getInetAddress().getHostAddress());

        try {
            Object objetoRecibido = ois.readObject();
            if (objetoRecibido instanceof Solicitud) {
                Solicitud solicitud = (Solicitud) objetoRecibido;
                System.out.println("ManejadorTareaWorker [" + nodoTrabajador.workerId
                        + "]: Solicitud de tarea recibida: " + solicitud.getTipoOperacion());

                Respuesta respuesta = nodoTrabajador.procesarSolicitud(solicitud);

                oos.writeObject(respuesta);
                oos.flush();
                System.out.println("ManejadorTareaWorker [" + nodoTrabajador.workerId
                        + "]: Respuesta enviada al ServidorCentral: " + respuesta.getEstado());
            } else {
                System.err.println("ManejadorTareaWorker [" + nodoTrabajador.workerId
                        + "]: Objeto recibido no es Solicitud: " + objetoRecibido.getClass().getName());
                // Opcional: enviar respuesta de error si es posible establecer comunicación de
                // vuelta
                Respuesta errorRespuesta = new Respuesta(EstadoOperacion.ERROR_COMUNICACION,
                        "Formato de tarea incorrecto.", null);
                try {
                    oos.writeObject(errorRespuesta);
                    oos.flush();
                } catch (IOException ioe) {
                    System.err.println("ManejadorTareaWorker [" + nodoTrabajador.workerId
                            + "]: Error al enviar respuesta de error de formato.");
                }
            }
        } catch (EOFException e) {
            System.out.println("ManejadorTareaWorker [" + nodoTrabajador.workerId
                    + "]: Conexión cerrada por ServidorCentral (EOF).");
        } catch (IOException e) {
            System.err.println("ManejadorTareaWorker [" + nodoTrabajador.workerId
                    + "]: IOException durante manejo de tarea: " + e.getMessage());
            // e.printStackTrace();
        } catch (ClassNotFoundException e) {
            System.err.println("ManejadorTareaWorker [" + nodoTrabajador.workerId + "]: ClassNotFoundException: "
                    + e.getMessage());
        } finally {
            cerrarRecursos();
        }
    }

    private void cerrarSocket() {
        try {
            if (socketTareaServidor != null && !socketTareaServidor.isClosed()) {
                socketTareaServidor.close();
            }
        } catch (IOException ex) {
            // System.err.println("ManejadorTareaWorker: Error al cerrar socket de tarea: "
            // + ex.getMessage());
        }
    }

    private void cerrarRecursos() {
        activo = false;
        System.out.println("ManejadorTareaWorker [" + nodoTrabajador.workerId + "]: Cerrando conexión de tarea con "
                + socketTareaServidor.getInetAddress().getHostAddress());
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
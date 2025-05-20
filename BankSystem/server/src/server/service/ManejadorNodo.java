package service;

import communication.Message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Clase que maneja las conexiones con los nodos trabajadores.
 * Recibe solicitudes del servidor central y envía respuestas.
 */
public class ManejadorNodo implements Runnable {
    private static final Logger logger = Logger.getLogger(ManejadorNodo.class.getName());

    private final Socket socketNodo;
    private final ServidorCentral servidor;
    private ObjectInputStream entrada;
    private ObjectOutputStream salida;
    private boolean conectado;
    private Integer nodoId;

    /**
     * Constructor del manejador de nodo
     * 
     * @param socketNodo Socket de conexión con el nodo
     * @param servidor   Referencia al servidor central
     */
    public ManejadorNodo(Socket socketNodo, ServidorCentral servidor) {
        this.socketNodo = socketNodo;
        this.servidor = servidor;
        this.conectado = true;
    }

    @Override
    public void run() {
        try {
            // Inicializar streams
            this.salida = new ObjectOutputStream(socketNodo.getOutputStream());
            this.entrada = new ObjectInputStream(socketNodo.getInputStream());

            // Esperar mensaje de registro del nodo
            Message mensajeRegistro = (Message) entrada.readObject();

            if (mensajeRegistro.getOperationType() == Message.OperationType.REGISTRO_NODO) {
                procesarRegistro(mensajeRegistro);

                // Procesar mensajes mientras el nodo esté conectado
                while (conectado) {
                    Message mensaje = (Message) entrada.readObject();
                    procesarMensaje(mensaje);
                }
            } else {
                logger.warning("No se recibió mensaje de registro válido del nodo");
                enviarMensaje(Message.error("Se esperaba un mensaje de registro"));
                cerrarConexion();
            }

        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.WARNING, "Conexión cerrada con el nodo: " + e.getMessage());
        } finally {
            cerrarConexion();
        }
    }

    /**
     * Procesa un mensaje de registro de nodo
     * 
     * @param mensaje El mensaje de registro
     * @throws IOException Si hay un error al enviar la confirmación
     */
    private void procesarRegistro(Message mensaje) throws IOException {
        nodoId = mensaje.getNodoId();
        String[] particiones = mensaje.getParticiones();

        logger.info("Nodo " + nodoId + " registrándose con " + particiones.length + " particiones");
        servidor.registrarNodo(nodoId, socketNodo, particiones);

        // Enviar confirmación
        Message confirmacion = new Message();
        confirmacion.setOperationType(Message.OperationType.CONFIRMACION_REGISTRO);
        enviarMensaje(confirmacion);
    }

    /**
     * Procesa un mensaje recibido del nodo
     * 
     * @param mensaje El mensaje a procesar
     */
    private void procesarMensaje(Message mensaje) {
        logger.info("Mensaje recibido del nodo " + nodoId + ": " + mensaje);

        try {
            switch (mensaje.getOperationType()) {
                case RESPUESTA_PROCESAR_CONSULTA:
                case RESPUESTA_VERIFICAR_SALDO:
                case RESPUESTA_ACTUALIZAR_SALDO:
                case RESPUESTA_REGISTRAR_TRANSACCION:
                    // Estas respuestas deberían procesarse en otras partes del código
                    // que envió la solicitud al nodo y está esperando respuesta.
                    logger.info("Respuesta recibida del nodo: " + mensaje.getOperationType());
                    break;

                case PING:
                    enviarMensaje(Message.error("PONG"));
                    break;

                default:
                    logger.warning("Operación del nodo no soportada: " + mensaje.getOperationType());
                    enviarMensaje(Message.error("Operación no soportada"));
                    break;
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error procesando mensaje del nodo", e);
            try {
                enviarMensaje(Message.error("Error en el servidor: " + e.getMessage()));
            } catch (IOException ioException) {
                logger.log(Level.SEVERE, "Error enviando mensaje de error al nodo", ioException);
                conectado = false;
            }
        }
    }

    /**
     * Envía un mensaje al nodo
     * 
     * @param mensaje El mensaje a enviar
     * @throws IOException Si hay un error de IO al enviar el mensaje
     */
    public void enviarMensaje(Message mensaje) throws IOException {
        salida.writeObject(mensaje);
        salida.flush();
    }

    /**
     * Cierra la conexión con el nodo
     */
    private void cerrarConexion() {
        conectado = false;
        try {
            if (entrada != null)
                entrada.close();
            if (salida != null)
                salida.close();
            if (socketNodo != null && !socketNodo.isClosed())
                socketNodo.close();
        } catch (IOException e) {
            logger.log(Level.WARNING, "Error cerrando la conexión con el nodo", e);
        }
        logger.info("Conexión con nodo " + nodoId + " cerrada");
    }
}
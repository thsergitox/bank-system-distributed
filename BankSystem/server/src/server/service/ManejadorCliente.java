package service;

import communication.Message;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Clase que maneja las conexiones entrantes de clientes.
 * Procesa las solicitudes y las redirige a los nodos adecuados.
 */
public class ManejadorCliente implements Runnable {
    private static final Logger logger = Logger.getLogger(ManejadorCliente.class.getName());

    private final Socket socketCliente;
    private final ServidorCentral servidor;
    private ObjectInputStream entrada;
    private ObjectOutputStream salida;
    private boolean conectado;

    /**
     * Constructor del manejador de clientes
     * 
     * @param socketCliente Socket de conexión con el cliente
     * @param servidor      Referencia al servidor central
     */
    public ManejadorCliente(Socket socketCliente, ServidorCentral servidor) {
        this.socketCliente = socketCliente;
        this.servidor = servidor;
        this.conectado = true;
    }

    @Override
    public void run() {
        try {
            // Inicializar streams
            this.salida = new ObjectOutputStream(socketCliente.getOutputStream());
            this.entrada = new ObjectInputStream(socketCliente.getInputStream());

            // Procesar mensajes mientras el cliente esté conectado
            while (conectado) {
                Message mensaje = (Message) entrada.readObject();
                procesarMensaje(mensaje);
            }

        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.WARNING, "Conexión cerrada con el cliente: " + e.getMessage());
        } finally {
            cerrarConexion();
        }
    }

    /**
     * Procesa un mensaje recibido del cliente
     * 
     * @param mensaje El mensaje a procesar
     */
    private void procesarMensaje(Message mensaje) {
        logger.info("Mensaje recibido: " + mensaje);

        try {
            switch (mensaje.getOperationType()) {
                case CONSULTAR_SALDO:
                    // TODO: Implementar lógica para consultar saldo
                    // 1. Identificar qué nodo tiene la cuenta
                    // 2. Enviar solicitud al nodo
                    // 3. Esperar respuesta
                    // 4. Transmitir respuesta al cliente

                    // Por ahora, enviamos una respuesta ficticia
                    Message respuestaSaldo = Message.respuestaSaldo(mensaje.getIdCuenta(), 1000.0);
                    enviarMensaje(respuestaSaldo);
                    break;

                case TRANSFERIR_FONDOS:
                    // TODO: Implementar lógica para transferir fondos
                    // 1. Verificar saldo en cuenta origen
                    // 2. Debitar cuenta origen
                    // 3. Acreditar cuenta destino
                    // 4. Registrar transacción

                    // Por ahora, enviamos una respuesta ficticia
                    Message respuestaTransferencia = Message.respuestaTransferencia(true,
                            "Transferencia procesada correctamente");
                    enviarMensaje(respuestaTransferencia);
                    break;

                case PING:
                    enviarMensaje(Message.error("PONG"));
                    break;

                default:
                    logger.warning("Operación no soportada: " + mensaje.getOperationType());
                    enviarMensaje(Message.error("Operación no soportada"));
                    break;
            }
        } catch (Exception e) {
            logger.log(Level.SEVERE, "Error procesando mensaje", e);
            try {
                enviarMensaje(Message.error("Error en el servidor: " + e.getMessage()));
            } catch (IOException ioException) {
                logger.log(Level.SEVERE, "Error enviando mensaje de error", ioException);
                conectado = false;
            }
        }
    }

    /**
     * Envía un mensaje al cliente
     * 
     * @param mensaje El mensaje a enviar
     * @throws IOException Si hay un error de IO al enviar el mensaje
     */
    private void enviarMensaje(Message mensaje) throws IOException {
        salida.writeObject(mensaje);
        salida.flush();
    }

    /**
     * Cierra la conexión con el cliente
     */
    private void cerrarConexion() {
        conectado = false;
        try {
            if (entrada != null)
                entrada.close();
            if (salida != null)
                salida.close();
            if (socketCliente != null && !socketCliente.isClosed())
                socketCliente.close();
        } catch (IOException e) {
            logger.log(Level.WARNING, "Error cerrando la conexión", e);
        }
        logger.info("Conexión con cliente cerrada");
    }
}
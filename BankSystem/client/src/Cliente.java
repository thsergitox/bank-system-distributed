import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Cliente que se conecta al Servidor Central y realiza operaciones bancarias
 */
public class Cliente {
    private static final Logger logger = Logger.getLogger(Cliente.class.getName());

    private final String servidor;
    private final int puerto;
    private Socket socket;
    private ObjectOutputStream salida;
    private ObjectInputStream entrada;
    private boolean conectado;

    /**
     * Constructor del cliente
     * 
     * @param servidor Dirección IP o hostname del servidor
     * @param puerto   Puerto del servidor
     */
    public Cliente(String servidor, int puerto) {
        this.servidor = servidor;
        this.puerto = puerto;
        this.conectado = false;
    }

    /**
     * Establece conexión con el servidor central
     * 
     * @return true si la conexión fue exitosa, false en caso contrario
     */
    public boolean conectar() {
        try {
            socket = new Socket(servidor, puerto);
            salida = new ObjectOutputStream(socket.getOutputStream());
            entrada = new ObjectInputStream(socket.getInputStream());
            conectado = true;
            logger.info("Conexión establecida con el servidor: " + servidor + ":" + puerto);
            return true;
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error conectando con el servidor", e);
            return false;
        }
    }

    /**
     * Cierra la conexión con el servidor
     */
    public void desconectar() {
        if (!conectado)
            return;

        try {
            conectado = false;
            if (salida != null)
                salida.close();
            if (entrada != null)
                entrada.close();
            if (socket != null)
                socket.close();
            logger.info("Conexión cerrada con el servidor");
        } catch (IOException e) {
            logger.log(Level.WARNING, "Error cerrando la conexión", e);
        }
    }

    /**
     * Consulta el saldo de una cuenta
     * 
     * @param idCuenta ID de la cuenta a consultar
     * @return El saldo de la cuenta o null en caso de error
     */
    public Double consultarSaldo(Long idCuenta) {
        if (!conectado) {
            logger.warning("No hay conexión con el servidor");
            return null;
        }

        try {
            // Crear mensaje de consulta
            Message mensaje = Message.consultarSaldo(idCuenta);

            // Enviar mensaje
            salida.writeObject(mensaje);
            salida.flush();

            // Esperar respuesta
            Message respuesta = (Message) entrada.readObject();

            if (respuesta.getOperationType() == Message.OperationType.RESPUESTA_SALDO) {
                logger.info("Saldo de cuenta " + idCuenta + ": " + respuesta.getSaldo());
                return respuesta.getSaldo();
            } else if (respuesta.getOperationType() == Message.OperationType.ERROR) {
                logger.warning("Error consultando saldo: " + respuesta.getMensaje());
                return null;
            } else {
                logger.warning("Respuesta inesperada del servidor: " + respuesta.getOperationType());
                return null;
            }
        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Error en la comunicación con el servidor", e);
            conectado = false;
            return null;
        }
    }

    /**
     * Transfiere dinero entre cuentas
     * 
     * @param origen  ID de la cuenta de origen
     * @param destino ID de la cuenta de destino
     * @param monto   Monto a transferir
     * @return true si la transferencia fue exitosa, false en caso contrario
     */
    public boolean transferir(Long origen, Long destino, Double monto) {
        if (!conectado) {
            logger.warning("No hay conexión con el servidor");
            return false;
        }

        try {
            // Crear mensaje de transferencia
            Message mensaje = Message.transferirFondos(origen, destino, monto);

            // Enviar mensaje
            salida.writeObject(mensaje);
            salida.flush();

            // Esperar respuesta
            Message respuesta = (Message) entrada.readObject();

            if (respuesta.getOperationType() == Message.OperationType.RESPUESTA_TRANSFERENCIA) {
                boolean exitosa = "Confirmada".equals(respuesta.getEstado());
                logger.info("Transferencia " + (exitosa ? "exitosa" : "fallida") +
                        ": " + respuesta.getMensaje());
                return exitosa;
            } else if (respuesta.getOperationType() == Message.OperationType.ERROR) {
                logger.warning("Error en transferencia: " + respuesta.getMensaje());
                return false;
            } else {
                logger.warning("Respuesta inesperada del servidor: " + respuesta.getOperationType());
                return false;
            }
        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Error en la comunicación con el servidor", e);
            conectado = false;
            return false;
        }
    }

    /**
     * Clase interna para representar un mensaje intercambiado con el servidor
     */
    public static class Message implements Serializable {
        private static final long serialVersionUID = 1L;

        public enum OperationType {
            CONSULTAR_SALDO,
            TRANSFERIR_FONDOS,
            RESPUESTA_SALDO,
            RESPUESTA_TRANSFERENCIA,
            ERROR,
            PING,
            PONG
        }

        private OperationType operationType;
        private Long idCuenta;
        private Long idCuentaOrigen;
        private Long idCuentaDestino;
        private Double monto;
        private Double saldo;
        private String mensaje;
        private String estado;

        // Constructor para consulta de saldo
        public static Message consultarSaldo(Long idCuenta) {
            Message message = new Message();
            message.operationType = OperationType.CONSULTAR_SALDO;
            message.idCuenta = idCuenta;
            return message;
        }

        // Constructor para transferencia
        public static Message transferirFondos(Long origen, Long destino, Double monto) {
            Message message = new Message();
            message.operationType = OperationType.TRANSFERIR_FONDOS;
            message.idCuentaOrigen = origen;
            message.idCuentaDestino = destino;
            message.monto = monto;
            return message;
        }

        // Getters y setters
        public OperationType getOperationType() {
            return operationType;
        }

        public Long getIdCuenta() {
            return idCuenta;
        }

        public Long getIdCuentaOrigen() {
            return idCuentaOrigen;
        }

        public Long getIdCuentaDestino() {
            return idCuentaDestino;
        }

        public Double getMonto() {
            return monto;
        }

        public Double getSaldo() {
            return saldo;
        }

        public String getMensaje() {
            return mensaje;
        }

        public String getEstado() {
            return estado;
        }
    }

    /**
     * Punto de entrada para cliente de simulación
     */
    public static void main(String[] args) {
        final String servidor = args.length >= 1 ? args[0] : "localhost";
        final int puerto = args.length >= 2 ? Integer.parseInt(args[1]) : 8000;
        final int numClientes = args.length >= 3 ? Integer.parseInt(args[2]) : 10;
        final int numTransacciones = args.length >= 4 ? Integer.parseInt(args[3]) : 50;

        // Crear pool de hilos para simular múltiples clientes
        ExecutorService pool = Executors.newFixedThreadPool(numClientes);

        for (int i = 0; i < numClientes; i++) {
            final int clienteId = i;

            pool.submit(() -> {
                try {
                    Cliente cliente = new Cliente(servidor, puerto);

                    if (cliente.conectar()) {
                        logger.info("Cliente " + clienteId + " conectado");

                        Random random = new Random();

                        // Realizar múltiples transacciones con delay aleatorio
                        for (int j = 0; j < numTransacciones; j++) {
                            try {
                                // Consultar saldo
                                if (random.nextBoolean()) {
                                    Long cuentaId = 100L + random.nextInt(100);
                                    Double saldo = cliente.consultarSaldo(cuentaId);
                                    logger.info(
                                            "Cliente " + clienteId + " - Cuenta " + cuentaId + " - Saldo: " + saldo);
                                }
                                // Transferir fondos
                                else {
                                    Long origen = 100L + random.nextInt(100);
                                    Long destino = 100L + random.nextInt(100);
                                    Double monto = 10.0 + random.nextDouble() * 90.0;
                                    monto = Math.round(monto * 100) / 100.0; // Redondear a 2 decimales

                                    boolean exitoso = cliente.transferir(origen, destino, monto);
                                    logger.info("Cliente " + clienteId + " - Transferencia " +
                                            (exitoso ? "exitosa" : "fallida") + ": " +
                                            origen + " -> " + destino + ": $" + monto);
                                }

                                // Delay aleatorio entre 100ms y 2s
                                Thread.sleep(100 + random.nextInt(1900));

                            } catch (InterruptedException e) {
                                logger.log(Level.WARNING, "Cliente " + clienteId + " interrumpido", e);
                                break;
                            }
                        }

                        cliente.desconectar();
                        logger.info("Cliente " + clienteId + " desconectado después de " +
                                numTransacciones + " transacciones");
                    }
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Error en cliente " + clienteId, e);
                }
            });
        }

        pool.shutdown();
    }
}
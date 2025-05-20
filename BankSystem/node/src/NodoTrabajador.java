import java.io.File;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Nodo trabajador que procesa operaciones sobre las particiones de datos
 * asignadas
 */
public class NodoTrabajador {
    private static final Logger logger = Logger.getLogger(NodoTrabajador.class.getName());

    private final String servidorCentral;
    private final int puerto;
    private final int nodoId;
    private final String directorioParticiones;
    private final int numThreads;
    private final ExecutorService threadPool;
    private final Map<String, Particion> particiones;

    private Socket socket;
    private ObjectOutputStream salida;
    private ObjectInputStream entrada;
    private boolean conectado;

    /**
     * Constructor del nodo trabajador
     * 
     * @param servidorCentral       Dirección del servidor central
     * @param puerto                Puerto del servidor central
     * @param nodoId                Identificador único del nodo
     * @param directorioParticiones Directorio donde se almacenan las particiones
     * @param numThreads            Número de hilos para procesar solicitudes
     */
    public NodoTrabajador(String servidorCentral, int puerto, int nodoId,
            String directorioParticiones, int numThreads) {
        this.servidorCentral = servidorCentral;
        this.puerto = puerto;
        this.nodoId = nodoId;
        this.directorioParticiones = directorioParticiones;
        this.numThreads = numThreads;
        this.threadPool = Executors.newFixedThreadPool(numThreads);
        this.particiones = new HashMap<>();
        this.conectado = false;
    }

    /**
     * Inicia el nodo trabajador, carga las particiones y se conecta al servidor
     * central
     */
    public void iniciar() {
        logger.info("Iniciando Nodo Trabajador ID: " + nodoId);

        // Cargar particiones
        cargarParticiones();

        // Conectarse al servidor central
        conectarAlServidor();

        // Registrar nodo con sus particiones
        if (conectado) {
            registrarNodo();
            iniciarProcesamientoMensajes();
        }
    }

    /**
     * Carga las particiones de datos desde archivos
     */
    private void cargarParticiones() {
        File directorio = new File(directorioParticiones);
        if (!directorio.exists() || !directorio.isDirectory()) {
            logger.warning("El directorio de particiones no existe: " + directorioParticiones);
            if (!directorio.mkdirs()) {
                logger.severe("No se pudo crear el directorio de particiones");
                return;
            }
            // Crear particiones de ejemplo para testing
            crearParticionesEjemplo();
            return;
        }

        File[] archivos = directorio.listFiles((dir, name) -> name.endsWith(".part"));
        if (archivos == null || archivos.length == 0) {
            logger.warning("No se encontraron particiones en " + directorioParticiones);
            // Crear particiones de ejemplo
            crearParticionesEjemplo();
            return;
        }

        for (File archivo : archivos) {
            try {
                String nombreParticion = archivo.getName().replace(".part", "");
                Particion particion = new Particion(archivo.getPath());
                particion.cargar();
                particiones.put(nombreParticion, particion);
                logger.info("Partición cargada: " + nombreParticion);
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error cargando partición: " + archivo.getName(), e);
            }
        }
    }

    /**
     * Crea particiones de ejemplo para testing
     */
    private void crearParticionesEjemplo() {
        logger.info("Creando particiones de ejemplo...");

        // Crear algunas cuentas y clientes de ejemplo
        try {
            // Ejemplo para la tabla Cuenta
            Particion partCuenta1 = new Particion(directorioParticiones + "/cuenta1.part");
            partCuenta1.setTipoTabla("CUENTA");
            partCuenta1.agregarRegistro("101,1,1500.00,Ahorros");
            partCuenta1.agregarRegistro("102,2,3200.50,Corriente");
            partCuenta1.guardar();
            particiones.put("cuenta1", partCuenta1);

            Particion partCuenta2 = new Particion(directorioParticiones + "/cuenta2.part");
            partCuenta2.setTipoTabla("CUENTA");
            partCuenta2.agregarRegistro("103,3,2500.75,Ahorros");
            partCuenta2.agregarRegistro("104,4,1200.30,Corriente");
            partCuenta2.guardar();
            particiones.put("cuenta2", partCuenta2);

            // Ejemplo para la tabla Cliente
            Particion partCliente1 = new Particion(directorioParticiones + "/cliente1.part");
            partCliente1.setTipoTabla("CLIENTE");
            partCliente1.agregarRegistro("1,Juan Pérez,juan@email.com,987654321");
            partCliente1.agregarRegistro("2,María López,maria@email.com,998877665");
            partCliente1.guardar();
            particiones.put("cliente1", partCliente1);

            logger.info("Particiones de ejemplo creadas con éxito");
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error creando particiones de ejemplo", e);
        }
    }

    /**
     * Establece conexión con el servidor central
     */
    private void conectarAlServidor() {
        try {
            socket = new Socket(servidorCentral, puerto);
            salida = new ObjectOutputStream(socket.getOutputStream());
            entrada = new ObjectInputStream(socket.getInputStream());
            conectado = true;
            logger.info("Conectado al servidor central: " + servidorCentral + ":" + puerto);
        } catch (IOException e) {
            logger.log(Level.SEVERE, "Error conectando al servidor central", e);
            conectado = false;
        }
    }

    /**
     * Registra el nodo y sus particiones en el servidor central
     */
    private void registrarNodo() {
        try {
            // Crear lista de nombres de particiones
            String[] nombresParticiones = particiones.keySet().toArray(new String[0]);

            // Construir mensaje de registro
            Message mensaje = Message.registroNodo(nodoId, nombresParticiones);

            // Enviar mensaje de registro
            salida.writeObject(mensaje);
            salida.flush();

            // Esperar confirmación
            Message respuesta = (Message) entrada.readObject();

            if (respuesta.getOperationType() == Message.OperationType.CONFIRMACION_REGISTRO) {
                logger.info("Registro exitoso en el servidor central");
            } else {
                logger.warning("Error en el registro: " + respuesta.getMensaje());
                conectado = false;
            }

        } catch (IOException | ClassNotFoundException e) {
            logger.log(Level.SEVERE, "Error en el registro del nodo", e);
            conectado = false;
        }
    }

    /**
     * Inicia el procesamiento de mensajes del servidor central
     */
    private void iniciarProcesamientoMensajes() {
        logger.info("Iniciando procesamiento de mensajes...");

        // Hilo para leer mensajes entrantes
        Thread hiloLectura = new Thread(() -> {
            while (conectado) {
                try {
                    Message mensaje = (Message) entrada.readObject();
                    procesarMensaje(mensaje);
                } catch (IOException | ClassNotFoundException e) {
                    if (conectado) {
                        logger.log(Level.SEVERE, "Error leyendo mensaje", e);
                        conectado = false;
                    }
                    break;
                }
            }
        });

        hiloLectura.start();
    }

    /**
     * Procesa un mensaje recibido del servidor central
     * 
     * @param mensaje El mensaje a procesar
     */
    private void procesarMensaje(Message mensaje) {
        logger.info("Mensaje recibido: " + mensaje.getOperationType());

        // Asignar a un hilo del pool para su procesamiento
        threadPool.execute(() -> {
            try {
                Message respuesta = null;

                switch (mensaje.getOperationType()) {
                    case PROCESAR_CONSULTA:
                        respuesta = procesarConsulta(mensaje);
                        break;

                    case VERIFICAR_SALDO:
                        respuesta = verificarSaldo(mensaje);
                        break;

                    case ACTUALIZAR_SALDO:
                        respuesta = actualizarSaldo(mensaje);
                        break;

                    case PING:
                        respuesta = Message.pong();
                        break;

                    default:
                        logger.warning("Operación no soportada: " + mensaje.getOperationType());
                        respuesta = Message.error("Operación no soportada");
                }

                enviarRespuesta(respuesta);

            } catch (Exception e) {
                logger.log(Level.SEVERE, "Error procesando mensaje", e);
                try {
                    enviarRespuesta(Message.error("Error procesando: " + e.getMessage()));
                } catch (IOException ex) {
                    logger.log(Level.SEVERE, "Error enviando respuesta de error", ex);
                }
            }
        });
    }

    /**
     * Procesa una consulta de saldo
     * 
     * @param mensaje El mensaje con la consulta
     * @return Un mensaje con la respuesta
     */
    private Message procesarConsulta(Message mensaje) {
        Long idCuenta = mensaje.getIdCuenta();
        logger.info("Procesando consulta de saldo para cuenta: " + idCuenta);

        // Buscar la cuenta en las particiones
        for (Particion particion : particiones.values()) {
            if ("CUENTA".equals(particion.getTipoTabla())) {
                String registro = particion.buscarRegistroPorId(String.valueOf(idCuenta));
                if (registro != null) {
                    String[] campos = registro.split(",");
                    if (campos.length >= 3) {
                        Double saldo = Double.parseDouble(campos[2]);
                        Message respuesta = new Message();
                        respuesta.setOperationType(Message.OperationType.RESPUESTA_PROCESAR_CONSULTA);
                        respuesta.setIdCuenta(idCuenta);
                        respuesta.setSaldo(saldo);
                        return respuesta;
                    }
                }
            }
        }

        // No se encontró la cuenta
        return Message.error("Cuenta no encontrada: " + idCuenta);
    }

    /**
     * Verifica el saldo de una cuenta
     * 
     * @param mensaje El mensaje con la solicitud
     * @return Un mensaje con la respuesta
     */
    private Message verificarSaldo(Message mensaje) {
        Long idCuenta = mensaje.getIdCuenta();
        Double montoRequerido = mensaje.getMonto();

        logger.info("Verificando saldo para cuenta: " + idCuenta +
                ", monto requerido: " + montoRequerido);

        // Buscar la cuenta en las particiones
        for (Particion particion : particiones.values()) {
            if ("CUENTA".equals(particion.getTipoTabla())) {
                String registro = particion.buscarRegistroPorId(String.valueOf(idCuenta));
                if (registro != null) {
                    String[] campos = registro.split(",");
                    if (campos.length >= 3) {
                        Double saldo = Double.parseDouble(campos[2]);
                        boolean saldoSuficiente = saldo >= montoRequerido;

                        Message respuesta = new Message();
                        respuesta.setOperationType(Message.OperationType.RESPUESTA_VERIFICAR_SALDO);
                        respuesta.setIdCuenta(idCuenta);
                        respuesta.setSaldo(saldo);
                        respuesta.setEstado(saldoSuficiente ? "SUFICIENTE" : "INSUFICIENTE");
                        return respuesta;
                    }
                }
            }
        }

        return Message.error("Cuenta no encontrada: " + idCuenta);
    }

    /**
     * Actualiza el saldo de una cuenta
     * 
     * @param mensaje El mensaje con la solicitud
     * @return Un mensaje con la respuesta
     */
    private Message actualizarSaldo(Message mensaje) {
        Long idCuenta = mensaje.getIdCuenta();
        Double monto = mensaje.getMonto(); // Puede ser positivo (incremento) o negativo (decremento)

        logger.info("Actualizando saldo para cuenta: " + idCuenta +
                ", monto: " + monto);

        // Buscar la cuenta en las particiones
        for (Particion particion : particiones.values()) {
            if ("CUENTA".equals(particion.getTipoTabla())) {
                String registro = particion.buscarRegistroPorId(String.valueOf(idCuenta));
                if (registro != null) {
                    String[] campos = registro.split(",");
                    if (campos.length >= 3) {
                        Double saldoActual = Double.parseDouble(campos[2]);
                        Double nuevoSaldo = saldoActual + monto;

                        if (nuevoSaldo < 0) {
                            return Message.error("Saldo insuficiente");
                        }

                        // Actualizar registro
                        String nuevoRegistro = campos[0] + "," + campos[1] + "," + nuevoSaldo + "," + campos[3];
                        particion.actualizarRegistro(registro, nuevoRegistro);
                        try {
                            particion.guardar(); // Persistir cambios
                        } catch (IOException e) {
                            logger.log(Level.SEVERE, "Error guardando partición", e);
                            return Message.error("Error guardando cambios: " + e.getMessage());
                        }

                        Message respuesta = new Message();
                        respuesta.setOperationType(Message.OperationType.RESPUESTA_ACTUALIZAR_SALDO);
                        respuesta.setIdCuenta(idCuenta);
                        respuesta.setSaldo(nuevoSaldo);
                        respuesta.setEstado("ACTUALIZADO");
                        return respuesta;
                    }
                }
            }
        }

        return Message.error("Cuenta no encontrada: " + idCuenta);
    }

    /**
     * Envía una respuesta al servidor central
     * 
     * @param respuesta El mensaje de respuesta
     * @throws IOException Si hay un error de IO
     */
    private synchronized void enviarRespuesta(Message respuesta) throws IOException {
        if (conectado) {
            salida.writeObject(respuesta);
            salida.flush();
        } else {
            throw new IOException("No hay conexión con el servidor central");
        }
    }

    /**
     * Detiene el nodo trabajador
     */
    public void detener() {
        logger.info("Deteniendo Nodo Trabajador...");

        conectado = false;
        threadPool.shutdown();

        try {
            if (entrada != null)
                entrada.close();
            if (salida != null)
                salida.close();
            if (socket != null)
                socket.close();
        } catch (IOException e) {
            logger.log(Level.WARNING, "Error cerrando conexiones", e);
        }

        logger.info("Nodo Trabajador detenido");
    }

    /**
     * Punto de entrada para el nodo trabajador
     */
    public static void main(String[] args) {
        String servidorCentral = "localhost";
        int puerto = 8001; // Puerto para nodos
        int nodoId = 1;
        String directorioParticiones = "data/nodo" + nodoId;
        int numThreads = Runtime.getRuntime().availableProcessors();

        // Permitir configuración por argumentos
        if (args.length >= 3) {
            servidorCentral = args[0];
            puerto = Integer.parseInt(args[1]);
            nodoId = Integer.parseInt(args[2]);
            directorioParticiones = "data/nodo" + nodoId;
        }
        if (args.length >= 4) {
            directorioParticiones = args[3];
        }
        if (args.length >= 5) {
            numThreads = Integer.parseInt(args[4]);
        }

        NodoTrabajador nodo = new NodoTrabajador(servidorCentral, puerto, nodoId,
                directorioParticiones, numThreads);
        nodo.iniciar();

        // Añadir un gancho de cierre para apagar el nodo limpiamente
        Runtime.getRuntime().addShutdownHook(new Thread(nodo::detener));
    }

    /**
     * Clase interna para representar un mensaje intercambiado con el servidor
     * central
     */
    public static class Message implements Serializable {
        private static final long serialVersionUID = 1L;

        public enum OperationType {
            // Operaciones de servidor a nodo
            PROCESAR_CONSULTA,
            PROCESAR_TRANSFERENCIA,
            VERIFICAR_SALDO,
            ACTUALIZAR_SALDO,
            REGISTRAR_TRANSACCION,

            // Respuestas de nodo a servidor
            RESPUESTA_PROCESAR_CONSULTA,
            RESPUESTA_PROCESAR_TRANSFERENCIA,
            RESPUESTA_VERIFICAR_SALDO,
            RESPUESTA_ACTUALIZAR_SALDO,
            RESPUESTA_REGISTRAR_TRANSACCION,

            // Mensajes de control
            ERROR,
            PING,
            PONG,
            REGISTRO_NODO,
            CONFIRMACION_REGISTRO
        }

        private OperationType operationType;
        private Long idCuenta;
        private Long idCuentaOrigen;
        private Long idCuentaDestino;
        private Double monto;
        private Double saldo;
        private String mensaje;
        private String estado;
        private Integer nodoId;
        private String[] particiones;

        // Constructor básico
        public Message() {
        }

        // Constructor para registro de nodo
        public static Message registroNodo(Integer nodoId, String[] particiones) {
            Message message = new Message();
            message.operationType = OperationType.REGISTRO_NODO;
            message.nodoId = nodoId;
            message.particiones = particiones;
            return message;
        }

        // Constructor para mensaje de error
        public static Message error(String mensaje) {
            Message message = new Message();
            message.operationType = OperationType.ERROR;
            message.mensaje = mensaje;
            return message;
        }

        // Constructor para mensaje de pong
        public static Message pong() {
            Message message = new Message();
            message.operationType = OperationType.PONG;
            return message;
        }

        // Getters y setters

        public OperationType getOperationType() {
            return operationType;
        }

        public void setOperationType(OperationType operationType) {
            this.operationType = operationType;
        }

        public Long getIdCuenta() {
            return idCuenta;
        }

        public void setIdCuenta(Long idCuenta) {
            this.idCuenta = idCuenta;
        }

        public Long getIdCuentaOrigen() {
            return idCuentaOrigen;
        }

        public void setIdCuentaOrigen(Long idCuentaOrigen) {
            this.idCuentaOrigen = idCuentaOrigen;
        }

        public Long getIdCuentaDestino() {
            return idCuentaDestino;
        }

        public void setIdCuentaDestino(Long idCuentaDestino) {
            this.idCuentaDestino = idCuentaDestino;
        }

        public Double getMonto() {
            return monto;
        }

        public void setMonto(Double monto) {
            this.monto = monto;
        }

        public Double getSaldo() {
            return saldo;
        }

        public void setSaldo(Double saldo) {
            this.saldo = saldo;
        }

        public String getMensaje() {
            return mensaje;
        }

        public void setMensaje(String mensaje) {
            this.mensaje = mensaje;
        }

        public String getEstado() {
            return estado;
        }

        public void setEstado(String estado) {
            this.estado = estado;
        }

        public Integer getNodoId() {
            return nodoId;
        }

        public void setNodoId(Integer nodoId) {
            this.nodoId = nodoId;
        }

        public String[] getParticiones() {
            return particiones;
        }

        public void setParticiones(String[] particiones) {
            this.particiones = particiones;
        }
    }

    /**
     * Clase que gestiona una partición de datos
     */
    private static class Particion {
        private final String rutaArchivo;
        private String tipoTabla;
        private final List<String> registros;

        public Particion(String rutaArchivo) {
            this.rutaArchivo = rutaArchivo;
            this.registros = new ArrayList<>();
        }

        public void cargar() throws IOException {
            File archivo = new File(rutaArchivo);
            if (archivo.exists()) {
                // Leer registros del archivo
                // ...
                // (Aquí iría la implementación real)
            }
        }

        public void guardar() throws IOException {
            // Guardar registros en el archivo
            // ...
            // (Aquí iría la implementación real)
        }

        public String buscarRegistroPorId(String id) {
            for (String registro : registros) {
                if (registro.startsWith(id + ",")) {
                    return registro;
                }
            }
            return null;
        }

        public void agregarRegistro(String registro) {
            registros.add(registro);
        }

        public void actualizarRegistro(String registroViejo, String registroNuevo) {
            int indice = registros.indexOf(registroViejo);
            if (indice >= 0) {
                registros.set(indice, registroNuevo);
            }
        }

        public String getTipoTabla() {
            return tipoTabla;
        }

        public void setTipoTabla(String tipoTabla) {
            this.tipoTabla = tipoTabla;
        }
    }
}
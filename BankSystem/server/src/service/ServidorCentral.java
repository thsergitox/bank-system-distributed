package service;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Servidor Central que coordina las comunicaciones entre clientes y nodos
 * trabajadores.
 * Actúa como un gateway y balanceador de carga.
 */
public class ServidorCentral {
    private static final Logger logger = Logger.getLogger(ServidorCentral.class.getName());

    private final int puertoCliente;
    private final int puertoNodo;
    private final ExecutorService poolClientes;
    private final ExecutorService poolNodos;
    private volatile boolean enEjecucion;

    // Mapeo de nodos disponibles y sus particiones
    private final Map<Integer, InformacionNodo> nodos;

    /**
     * Constructor del Servidor Central
     * 
     * @param puertoCliente      Puerto para conexiones de clientes
     * @param puertoNodo         Puerto para conexiones de nodos
     * @param maxThreadsClientes Número máximo de hilos para atender clientes
     * @param maxThreadsNodos    Número máximo de hilos para atender nodos
     */
    public ServidorCentral(int puertoCliente, int puertoNodo, int maxThreadsClientes, int maxThreadsNodos) {
        this.puertoCliente = puertoCliente;
        this.puertoNodo = puertoNodo;
        this.poolClientes = Executors.newFixedThreadPool(maxThreadsClientes);
        this.poolNodos = Executors.newFixedThreadPool(maxThreadsNodos);
        this.nodos = new ConcurrentHashMap<>();
        this.enEjecucion = false;
    }

    /**
     * Inicia el servidor central en dos hilos separados: uno para clientes y otro
     * para nodos
     */
    public void iniciar() {
        enEjecucion = true;

        // Hilo para atender conexiones de clientes
        Thread hiloClientes = new Thread(() -> {
            try (ServerSocket serverSocketCliente = new ServerSocket(puertoCliente)) {
                logger.info("Servidor Central escuchando conexiones de clientes en el puerto " + puertoCliente);

                while (enEjecucion) {
                    try {
                        Socket socketCliente = serverSocketCliente.accept();
                        logger.info("Nueva conexión de cliente: " + socketCliente.getInetAddress());

                        // Asignar la conexión a un hilo del pool
                        poolClientes.execute(new ManejadorCliente(socketCliente, this));
                    } catch (IOException e) {
                        if (enEjecucion) {
                            logger.log(Level.SEVERE, "Error aceptando conexión de cliente", e);
                        }
                    }
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error iniciando servidor para clientes", e);
            }
        });

        // Hilo para atender conexiones de nodos
        Thread hiloNodos = new Thread(() -> {
            try (ServerSocket serverSocketNodo = new ServerSocket(puertoNodo)) {
                logger.info("Servidor Central escuchando conexiones de nodos en el puerto " + puertoNodo);

                while (enEjecucion) {
                    try {
                        Socket socketNodo = serverSocketNodo.accept();
                        logger.info("Nueva conexión de nodo: " + socketNodo.getInetAddress());

                        // Asignar la conexión a un hilo del pool
                        poolNodos.execute(new ManejadorNodo(socketNodo, this));
                    } catch (IOException e) {
                        if (enEjecucion) {
                            logger.log(Level.SEVERE, "Error aceptando conexión de nodo", e);
                        }
                    }
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, "Error iniciando servidor para nodos", e);
            }
        });

        // Iniciar ambos hilos
        hiloClientes.start();
        hiloNodos.start();
    }

    /**
     * Detiene el servidor central y libera recursos
     */
    public void detener() {
        logger.info("Deteniendo Servidor Central...");
        enEjecucion = false;

        poolClientes.shutdown();
        poolNodos.shutdown();

        logger.info("Servidor Central detenido");
    }

    /**
     * Registra un nuevo nodo en el sistema
     * 
     * @param nodoId      ID único del nodo
     * @param conexion    Información de conexión del nodo
     * @param particiones Lista de particiones que maneja el nodo
     */
    public void registrarNodo(int nodoId, Socket conexion, String[] particiones) {
        InformacionNodo infoNodo = new InformacionNodo(nodoId, conexion, particiones);
        nodos.put(nodoId, infoNodo);
        logger.info("Nodo registrado: " + nodoId + " con " + particiones.length + " particiones");
    }

    /**
     * Clase interna para almacenar información de un nodo
     */
    private static class InformacionNodo {
        private final int id;
        private final Socket conexion;
        private final String[] particiones;

        public InformacionNodo(int id, Socket conexion, String[] particiones) {
            this.id = id;
            this.conexion = conexion;
            this.particiones = particiones;
        }
    }

    /**
     * Punto de entrada del programa
     */
    public static void main(String[] args) {
        int puertoCliente = 8000;
        int puertoNodo = 8001;
        int maxThreadsClientes = 100;
        int maxThreadsNodos = 20;

        // Permitir configuración por argumentos
        if (args.length >= 2) {
            puertoCliente = Integer.parseInt(args[0]);
            puertoNodo = Integer.parseInt(args[1]);
        }
        if (args.length >= 4) {
            maxThreadsClientes = Integer.parseInt(args[2]);
            maxThreadsNodos = Integer.parseInt(args[3]);
        }

        ServidorCentral servidor = new ServidorCentral(puertoCliente, puertoNodo, maxThreadsClientes, maxThreadsNodos);
        servidor.iniciar();

        // Añadir un gancho de cierre para apagar el servidor limpiamente
        Runtime.getRuntime().addShutdownHook(new Thread(servidor::detener));
    }
}
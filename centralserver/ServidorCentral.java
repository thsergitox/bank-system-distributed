package centralserver;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import commons.InfoWorker; // Importar InfoWorker
import commons.Cliente;
import commons.Cuenta;
import commons.EstadoOperacion;

public class ServidorCentral {

    private static final int PUERTO_CLIENTES = 12345;
    private static final int PUERTO_WORKERS = 12346; // Nuevo puerto para workers
    private static final int MAX_CLIENTES_CONCURRENTES = 50;
    private static final int MAX_WORKERS_CONCURRENTES = 20; // Límite para workers
    private static final int NUM_PARTICIONES_CUENTAS = 3;
    private static final int NUM_PARTICIONES_CLIENTES = 3;
    private static final int MAX_REPLICAS_POR_PARTICION = 3;
    private static final String LOG_TRANSACCIONES_GLOBALES = "../data" + File.separator + "transacciones_globales.log";
    private static AtomicInteger contadorIdTransaccionGlobal = new AtomicInteger(0);

    // Estructuras para manejar workers y particiones
    // Usamos ConcurrentHashMap para seguridad en hilos
    public static final ConcurrentHashMap<String, InfoWorker> workersActivos = new ConcurrentHashMap<>();
    // Mapea: ID_Particion -> Lista de IDs de Workers que la tienen
    public static final ConcurrentHashMap<String, List<String>> particionANodos = new ConcurrentHashMap<>();
    // Mapea: ID_Cuenta -> ID_Particion (Ej: 101 -> "CUENTA_P1")
    public static final ConcurrentHashMap<Integer, String> cuentaAParticion = new ConcurrentHashMap<>();
    public static final ConcurrentHashMap<Integer, String> clienteAParticion = new ConcurrentHashMap<>();

    // Almacenará los objetos de cada partición para enviarlos a los workers cuando
    // se registren.
    // Clave: ID de Partición (ej: "CUENTA_P1"), Valor: Lista de objetos (Cuenta o
    // Cliente)
    public static final ConcurrentHashMap<String, List<? extends Serializable>> datosParticionesGlobales = new ConcurrentHashMap<>();

    // Método para registrar/actualizar un worker y sus particiones
    // Debe ser synchronized para proteger el acceso concurrente a particionANodos y
    // cuentaAParticion
    public static synchronized void registrarActualizarWorker(InfoWorker infoWorker,
            List<String> particionesAsignadas) {
        infoWorker.setParticionesManejadas(particionesAsignadas);
        workersActivos.put(infoWorker.getWorkerId(), infoWorker);
        System.out.println("ServidorCentral: Worker " + infoWorker.getWorkerId() + " registrado/actualizado. Host: "
                + infoWorker.getHost() + ", Puerto Tareas: " + infoWorker.getPuertoTareas() + ", Particiones: "
                + particionesAsignadas);

        for (String particionId : particionesAsignadas) {
            particionANodos.computeIfAbsent(particionId, a -> new ArrayList<>()).remove(infoWorker.getWorkerId()); // Evitar
                                                                                                                   // duplicados
                                                                                                                   // si
                                                                                                                   // se
                                                                                                                   // re-registra
            particionANodos.computeIfAbsent(particionId, a -> new ArrayList<>()).add(infoWorker.getWorkerId());
            System.out.println("ServidorCentral: Worker " + infoWorker.getWorkerId() + " ahora maneja partición "
                    + particionId);
        }
    }

    // Método para desregistrar un worker
    public static synchronized void desregistrarWorker(String workerId) {
        InfoWorker info = workersActivos.remove(workerId);
        if (info != null) {
            System.out.println("ServidorCentral: Worker " + workerId + " desregistrado.");
            if (info.getParticionesManejadas() != null) {
                for (String particionId : info.getParticionesManejadas()) {
                    List<String> workers = particionANodos.get(particionId);
                    if (workers != null) {
                        boolean removed = workers.remove(workerId); // Asegurarse que la remoción sea efectiva
                        if (removed)
                            System.out.println("ServidorCentral: Worker " + workerId
                                    + " explícitamente removido de la lista de la partición " + particionId);
                        if (workers.isEmpty()) {
                            particionANodos.remove(particionId); // Si ya no hay workers para esta partición
                            System.out.println(
                                    "ServidorCentral: Partición " + particionId + " ya no tiene workers asignados.");
                        }
                    }
                }
            }
        } else {
            System.out.println("ServidorCentral: Intento de desregistrar worker " + workerId
                    + " que no estaba activo o ya fue desregistrado.");
        }
    }

    /**
     * Asigna particiones a un worker que se registra. Intenta asignar particiones
     * que tengan
     * menos de MAX_REPLICAS_POR_PARTICION.
     * Llena las listas 'particionesAsignadasAlWorkerParam' y
     * 'datosParaWorkerParam'.
     */
    public static synchronized void asignarParticionesAWorker(String workerId,
            List<String> particionesAsignadasAlWorkerParam,
            Map<String, List<? extends Serializable>> datosParaWorkerParam) {
        System.out.println("ServidorCentral: Iniciando asignación de particiones para worker " + workerId);
        int particionesAsignadasEnEstaRonda = 0;

        List<String> todasLasIdsParticiones = new ArrayList<>(datosParticionesGlobales.keySet());
        Collections.shuffle(todasLasIdsParticiones); // Para distribuir un poco al azar si varios workers se registran
                                                     // al mismo tiempo

        for (String idParticionGlobal : todasLasIdsParticiones) {
            List<String> workersConEstaParticion = particionANodos.getOrDefault(idParticionGlobal, new ArrayList<>());

            // Verificar si este worker ya tiene esta partición (por si se re-registra o por
            // error)
            if (workersConEstaParticion.contains(workerId)) {
                continue;
            }

            if (workersConEstaParticion.size() < MAX_REPLICAS_POR_PARTICION) {
                List<? extends Serializable> datosDeLaParticion = datosParticionesGlobales.get(idParticionGlobal);
                if (datosDeLaParticion != null && !datosDeLaParticion.isEmpty()) {
                    particionesAsignadasAlWorkerParam.add(idParticionGlobal);
                    datosParaWorkerParam.put(idParticionGlobal, datosDeLaParticion);
                    System.out.println("ServidorCentral: Asignando partición " + idParticionGlobal + " a worker "
                            + workerId + " (Réplica #" + (workersConEstaParticion.size() + 1) + ")");
                    particionesAsignadasEnEstaRonda++;
                    // No actualizamos particionANodos aquí todavía, solo cuando el worker confirme
                    // recepción
                } else {
                    System.err.println(
                            "ServidorCentral: Partición global " + idParticionGlobal + " no tiene datos cargados.");
                }
                // if (particionesAsignadasEnEstaRonda >= maxParticionesPorWorker) break; //
                // Opcional: limitar asignaciones por ronda
            } else {
                System.out.println("ServidorCentral: Partición " + idParticionGlobal + " ya tiene "
                        + workersConEstaParticion.size() + " réplicas. No se asigna a " + workerId);
            }
        }
        if (particionesAsignadasEnEstaRonda == 0) {
            System.out.println("ServidorCentral: No se asignaron nuevas particiones a worker " + workerId
                    + " en esta ronda (posiblemente todas las particiones ya tienen suficientes réplicas o no hay datos).");
        }
    }

    /**
     * Usado si un worker falla al confirmar la recepción de datos, para limpiar las
     * asignaciones
     * que se hicieron tentativamente en particionANodos (si se hubieran hecho
     * allí).
     * Actualmente, la asignación a particionANodos se hace en
     * registrarActualizarWorker.
     */
    public static synchronized void removerParticionesDeWorker(String workerId,
            List<String> particionesQueSeIntentaronAsignar) {
        if (workerId == null || particionesQueSeIntentaronAsignar == null)
            return;
        System.out.println("ServidorCentral: Revirtiendo asignación tentativa de particiones para worker " + workerId
                + ": " + particionesQueSeIntentaronAsignar);
        for (String idParticion : particionesQueSeIntentaronAsignar) {
            List<String> workers = particionANodos.get(idParticion);
            if (workers != null) {
                boolean removed = workers.remove(workerId);
                if (removed)
                    System.out.println(
                            "ServidorCentral: Worker " + workerId + " removido de partición tentativa " + idParticion);
                if (workers.isEmpty()) {
                    particionANodos.remove(idParticion);
                }
            }
        }
    }

    public static synchronized int generarIdTransaccionGlobal() {
        return contadorIdTransaccionGlobal.incrementAndGet();
    }

    public static synchronized void registrarTransaccionGlobal(int idTransaccion, int idCuentaOrigen,
            int idCuentaDestino, double monto, EstadoOperacion estadoFinal, String detalle) {
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(LOG_TRANSACCIONES_GLOBALES, true))) {
            // ID_TRANSACC|ID_ORIG|ID_DEST|MONTO|FECHA_HORA|ESTADO|DETALLE
            String lineaLog = String.format("%d|%d|%d|%.2f|%s|%s|%s\n",
                    idTransaccion,
                    idCuentaOrigen,
                    idCuentaDestino,
                    monto,
                    new Date().toString(), // Usar java.util.Date para la fecha y hora actual
                    estadoFinal.toString(),
                    detalle).replace(',', '.');
            bw.write(lineaLog);
            System.out.println("ServidorCentral: Transacción global registrada: " + lineaLog.trim());
        } catch (IOException e) {
            System.err
                    .println("ServidorCentral: Error al escribir en log de transacciones globales: " + e.getMessage());
        }
    }

    public static void main(String[] args) {
        System.out.println("Servidor Central iniciando...");
        // Crear archivo de log de transacciones si no existe y añadir cabecera
        try (BufferedWriter bw = new BufferedWriter(new FileWriter(LOG_TRANSACCIONES_GLOBALES, true))) {
            File logFile = new File(LOG_TRANSACCIONES_GLOBALES);
            if (logFile.length() == 0) { // Escribir cabecera solo si el archivo está vacío
                bw.write("ID_TRANSACC|ID_ORIG|ID_DEST|MONTO|FECHA_HORA|ESTADO|DETALLE\n");
            }
        } catch (IOException e) {
            System.err
                    .println("ServidorCentral: Error al inicializar log de transacciones globales: " + e.getMessage());
        }
        cargarYParticionarDatosGlobales();

        ServidorCentral servidor = new ServidorCentral();
        new Thread(servidor::escucharWorkers).start(); // Lanza el listener de workers en un nuevo hilo
        servidor.escucharClientes(); // El listener de clientes se ejecuta en el hilo main
    }

    private void escucharClientes() {
        ExecutorService poolClientes = Executors.newFixedThreadPool(MAX_CLIENTES_CONCURRENTES);
        System.out.println("Servidor Central: Escuchando clientes en el puerto " + PUERTO_CLIENTES + "...");

        try (ServerSocket serverSocket = new ServerSocket(PUERTO_CLIENTES)) {
            while (true) {
                try {
                    Socket socketCliente = serverSocket.accept();
                    System.out.println("Servidor Central: Nuevo cliente conectado: "
                            + socketCliente.getInetAddress().getHostAddress());
                    poolClientes.execute(new ManejadorClienteServidor(socketCliente, this));
                } catch (IOException e) {
                    System.err.println("Servidor Central: Error al aceptar conexión de cliente: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Servidor Central: Error CRÍTICO al iniciar ServerSocket para clientes en puerto "
                    + PUERTO_CLIENTES + ": " + e.getMessage());
        }
    }

    private void escucharWorkers() {
        ExecutorService poolWorkers = Executors.newFixedThreadPool(MAX_WORKERS_CONCURRENTES);
        System.out.println("Servidor Central: Escuchando workers en el puerto " + PUERTO_WORKERS + "...");

        try (ServerSocket serverSocketWorkers = new ServerSocket(PUERTO_WORKERS)) {
            while (true) {
                try {
                    Socket socketWorker = serverSocketWorkers.accept();
                    System.out.println("Servidor Central: Nuevo worker conectado: "
                            + socketWorker.getInetAddress().getHostAddress());
                    poolWorkers.execute(new ManejadorWorkerServidor(socketWorker, this)); // Pasamos 'this'
                } catch (IOException e) {
                    System.err.println("Servidor Central: Error al aceptar conexión de worker: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Servidor Central: Error CRÍTICO al iniciar ServerSocket para workers en puerto "
                    + PUERTO_WORKERS + ": " + e.getMessage());
        }
    }

    private static void cargarYParticionarDatosGlobales() {
        System.out.println("ServidorCentral: Cargando y particionando datos globales...");
        List<Cliente> todosLosClientes = new ArrayList<>();
        List<Cuenta> todasLasCuentas = new ArrayList<>();

        // Cargar Clientes
        try (BufferedReader br = new BufferedReader(new FileReader("../data/clientes.csv"))) {
            String linea;
            br.readLine(); // Saltar cabecera
            while ((linea = br.readLine()) != null) {
                String[] p = linea.split("\\|");
                if (p.length >= 4) {
                    todosLosClientes.add(new Cliente(Integer.parseInt(p[0]), p[1], p[2], p[3]));
                }
            }
        } catch (IOException e) {
            System.err.println("ServidorCentral: Error al leer data/clientes.csv: " + e.getMessage());
        }
        System.out.println("ServidorCentral: Cargados " + todosLosClientes.size() + " clientes.");

        // Cargar Cuentas
        try (BufferedReader br = new BufferedReader(new FileReader("../data/cuentas.csv"))) {
            String linea;
            br.readLine(); // Saltar cabecera
            while ((linea = br.readLine()) != null) {
                String[] p = linea.split("\\|");
                if (p.length >= 4) {
                    todasLasCuentas.add(
                            new Cuenta(Integer.parseInt(p[0]), Integer.parseInt(p[1]), Double.parseDouble(p[2]), p[3]));
                }
            }
        } catch (IOException e) {
            System.err.println("ServidorCentral: Error al leer data/cuentas.csv: " + e.getMessage());
        }
        System.out.println("ServidorCentral: Cargadas " + todasLasCuentas.size() + " cuentas.");

        // Particionar Clientes
        int tamParticionClientes = (int) Math.ceil((double) todosLosClientes.size() / NUM_PARTICIONES_CLIENTES);
        for (int i = 0; i < NUM_PARTICIONES_CLIENTES; i++) {
            String idParticion = "CLIENTE_P" + (i + 1);
            List<Cliente> clientesParticion = new ArrayList<>();
            for (int j = i * tamParticionClientes; j < (i + 1) * tamParticionClientes
                    && j < todosLosClientes.size(); j++) {
                Cliente cliente = todosLosClientes.get(j);
                clientesParticion.add(cliente);
                clienteAParticion.put(cliente.getIdCliente(), idParticion);
            }
            datosParticionesGlobales.put(idParticion, clientesParticion);
            System.out.println("ServidorCentral: Creada partición " + idParticion + " con " + clientesParticion.size()
                    + " clientes.");
        }

        // Particionar Cuentas
        int tamParticionCuentas = (int) Math.ceil((double) todasLasCuentas.size() / NUM_PARTICIONES_CUENTAS);
        for (int i = 0; i < NUM_PARTICIONES_CUENTAS; i++) {
            String idParticion = "CUENTA_P" + (i + 1);
            List<Cuenta> cuentasParticion = new ArrayList<>();
            for (int j = i * tamParticionCuentas; j < (i + 1) * tamParticionCuentas
                    && j < todasLasCuentas.size(); j++) {
                Cuenta cuenta = todasLasCuentas.get(j);
                cuentasParticion.add(cuenta);
                cuentaAParticion.put(cuenta.getIdCuenta(), idParticion);
            }
            datosParticionesGlobales.put(idParticion, cuentasParticion);
            System.out.println("ServidorCentral: Creada partición " + idParticion + " con " + cuentasParticion.size()
                    + " cuentas.");
        }
        // La tabla de Transacciones no se pre-particionará activamente para delegación,
        // se registrará centralmente o por evento.
        System.out.println("ServidorCentral: Datos globales cargados y particionados.");
    }

    // Más adelante: métodos para inicializar/cargar metadatos de particiones
    // private static void inicializarMetadatosParticiones() { ... }

    // Para que ManejadorClienteServidor pueda acceder a los datos de partición para
    // enviar a workers
    // Esto es solo si la delegación de la tarea la hace el
    // ManejadorClienteServidor,
    // si la hace el ManejadorWorkerServidor al registrarse, este método no es
    // necesario aquí.
    public static List<? extends Serializable> getDatosDeParticion(String idParticion) {
        return datosParticionesGlobales.get(idParticion);
    }
}
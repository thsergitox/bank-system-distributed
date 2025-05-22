package client;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.Scanner;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import commons.Solicitud;
import commons.Respuesta;
import commons.TipoOperacion;

public class Cliente {
    private static String HOST_SERVIDOR = "localhost";
    private static int PUERTO_SERVIDOR = 12345;
    private static final Random random = new Random();

    // Rango de IDs de cuenta para las operaciones
    private static final int MIN_ID_CUENTA = 100001;
    private static final int MAX_ID_CUENTA = 106049;

    static class TareaCliente implements Runnable {
        private int idClienteSimulado;
        private int numOperaciones;

        public TareaCliente(int idClienteSimulado, int numOperaciones) {
            this.idClienteSimulado = idClienteSimulado;
            this.numOperaciones = numOperaciones;
        }

        private int generarIdCuentaAleatorio() {
            return MIN_ID_CUENTA + random.nextInt(MAX_ID_CUENTA - MIN_ID_CUENTA + 1);
        }

        @Override
        public void run() {
            System.out.println("Cliente Simulado [" + idClienteSimulado + "]: Iniciando...");
            try (Socket socket = new Socket(HOST_SERVIDOR, PUERTO_SERVIDOR);
                    ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                    ObjectInputStream ois = new ObjectInputStream(socket.getInputStream())) {

                System.out.println("Cliente Simulado [" + idClienteSimulado + "]: Conectado al Servidor Central.");

                for (int i = 0; i < numOperaciones; i++) {
                    realizarOperacionAleatoria(oos, ois, idClienteSimulado, i + 1);
                    try {
                        Thread.sleep(random.nextInt(200) + 50); // Reducir delay para más carga: 50-250ms
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        System.err.println("Cliente Simulado [" + idClienteSimulado + "]: Hilo interrumpido.");
                        break;
                    }
                }
            } catch (UnknownHostException e) {
                System.err.println("Cliente Simulado [" + idClienteSimulado + "]: Host del servidor no encontrado: "
                        + HOST_SERVIDOR);
            } catch (IOException e) {
                System.err.println("Cliente Simulado [" + idClienteSimulado + "]: Error de IO - " + e.getMessage());
            } catch (Exception e) {
                System.err
                        .println("Cliente Simulado [" + idClienteSimulado + "]: Error inesperado - " + e.getMessage());
                // e.printStackTrace(); // Descomentar para debugging detallado
            }
            System.out.println("Cliente Simulado [" + idClienteSimulado + "]: Finalizado.");
        }

        private void realizarOperacionAleatoria(ObjectOutputStream oos, ObjectInputStream ois, int clienteId, int opNum)
                throws IOException, ClassNotFoundException {
            Solicitud solicitud;
            Map<String, Object> parametros = new HashMap<>();

            int operacionRandom = random.nextInt(100); // 60% consultas, 38% transferencias, 2% Arqueo

            if (operacionRandom < 60) { // CONSULTAR_SALDO
                int cuentaAConsultar = generarIdCuentaAleatorio();
                parametros.put("ID_CUENTA", cuentaAConsultar);
                solicitud = new Solicitud(TipoOperacion.CONSULTAR_SALDO, parametros);
                System.out.println("Cliente [" + clienteId + ", Op#" + opNum + "]: CONSULTAR_SALDO para cuenta "
                        + cuentaAConsultar);
            } else if (operacionRandom < 98) { // TRANSFERIR_FONDOS
                int idCuentaOrigen = generarIdCuentaAleatorio();
                int idCuentaDestino = generarIdCuentaAleatorio();
                while (idCuentaDestino == idCuentaOrigen) {
                    idCuentaDestino = generarIdCuentaAleatorio();
                }
                double monto = Math.round((random.nextDouble() * 200 + 5.0) * 100.0) / 100.0; // Monto entre 5.00 y
                                                                                              // 205.00

                parametros.put("ID_CUENTA_ORIGEN", idCuentaOrigen);
                parametros.put("ID_CUENTA_DESTINO", idCuentaDestino);
                parametros.put("MONTO", monto);
                solicitud = new Solicitud(TipoOperacion.TRANSFERIR_FONDOS, parametros);
                System.out.println("Cliente [" + clienteId + ", Op#" + opNum + "]: TRANSFERIR_FONDOS de "
                        + idCuentaOrigen + " a " + idCuentaDestino + " por " + monto);
            } else { // ARQUEO_CUENTAS
                solicitud = new Solicitud(TipoOperacion.ARQUEO_CUENTAS, new HashMap<>());
                System.out.println("Cliente [" + clienteId + ", Op#" + opNum + "]: SOLICITANDO ARQUEO_CUENTAS");
            }

            enviarYRecibir(solicitud, oos, ois, clienteId + ", Op#" + opNum + " " + solicitud.getTipoOperacion());
        }
    }

    public static void main(String[] args) {
        int numClientes = 5;
        int numOpsPorCliente = 10;
        boolean modoInteractivo = false;

        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-h":
                case "--host":
                    if (i + 1 < args.length)
                        HOST_SERVIDOR = args[++i];
                    else {
                        System.err.println("Falta el valor para -h/--host");
                        return;
                    }
                    break;
                case "-p":
                case "--port":
                    if (i + 1 < args.length)
                        PUERTO_SERVIDOR = Integer.parseInt(args[++i]);
                    else {
                        System.err.println("Falta el valor para -p/--port");
                        return;
                    }
                    break;
                case "-c":
                case "--clients":
                    if (i + 1 < args.length)
                        numClientes = Integer.parseInt(args[++i]);
                    else {
                        System.err.println("Falta el valor para -c/--clients");
                        return;
                    }
                    break;
                case "-o":
                case "--ops":
                    if (i + 1 < args.length)
                        numOpsPorCliente = Integer.parseInt(args[++i]);
                    else {
                        System.err.println("Falta el valor para -o/--ops");
                        return;
                    }
                    break;
                case "-i":
                case "--interactive":
                    modoInteractivo = true;
                    break;
                default:
                    System.err.println("Opción desconocida: " + args[i]);
                    System.err.println(
                            "Uso: java Cliente [-h host] [-p puerto] [-c numClientes] [-o numOpsPorCliente] [-i para modo interactivo]");
                    return;
            }
        }

        if (modoInteractivo) {
            ejecutarModoInteractivo();
        } else {
            ejecutarModoSimulacion(numClientes, numOpsPorCliente);
        }
    }

    private static void ejecutarModoInteractivo() {
        System.out.println("Cliente en Modo Interactivo. Conectando a " + HOST_SERVIDOR + ":" + PUERTO_SERVIDOR);
        try (Socket socket = new Socket(HOST_SERVIDOR, PUERTO_SERVIDOR);
                ObjectOutputStream oos = new ObjectOutputStream(socket.getOutputStream());
                ObjectInputStream ois = new ObjectInputStream(socket.getInputStream());
                Scanner scanner = new Scanner(System.in)) {

            System.out.println("Conectado al Servidor Central.");
            while (true) {
                System.out.println("\nSeleccione una operación:");
                System.out.println("1. Consultar Saldo");
                System.out.println("2. Transferir Fondos");
                System.out.println("3. Realizar Arqueo de Cuentas");
                System.out.println("4. Salir");
                System.out.print("Opción: ");
                String opcion = scanner.nextLine().trim();

                if ("4".equals(opcion))
                    break;

                Solicitud solicitud = null;
                Map<String, Object> parametros;

                try {
                    switch (opcion) {
                        case "1":
                            System.out.print("Ingrese ID de Cuenta: ");
                            int idCuentaConsulta = Integer.parseInt(scanner.nextLine().trim());
                            parametros = Map.of("ID_CUENTA", idCuentaConsulta);
                            solicitud = new Solicitud(TipoOperacion.CONSULTAR_SALDO, parametros);
                            break;
                        case "2":
                            System.out.print("ID Cuenta Origen: ");
                            int idO = Integer.parseInt(scanner.nextLine().trim());
                            System.out.print("ID Cuenta Destino: ");
                            int idD = Integer.parseInt(scanner.nextLine().trim());
                            System.out.print("Monto: ");
                            double monto = Double.parseDouble(scanner.nextLine().trim().replace(',', '.'));
                            parametros = Map.of("ID_CUENTA_ORIGEN", idO, "ID_CUENTA_DESTINO", idD, "MONTO", monto);
                            solicitud = new Solicitud(TipoOperacion.TRANSFERIR_FONDOS, parametros);
                            break;
                        case "3":
                            solicitud = new Solicitud(TipoOperacion.ARQUEO_CUENTAS, new HashMap<>());
                            break;
                        default:
                            System.out.println("Opción inválida.");
                            continue;
                    }
                    if (solicitud != null)
                        enviarYRecibir(solicitud, oos, ois, "Interactivo");
                } catch (NumberFormatException e) {
                    System.err.println("Error: Entrada numérica no válida - " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.err.println("Error en cliente interactivo: " + e.getMessage());
            // e.printStackTrace();
        }
        System.out.println("Cliente interactivo desconectado.");
    }

    private static void ejecutarModoSimulacion(int numClientes, int numOpsPorCliente) {
        System.out.println("Iniciando simulación con " + numClientes + " clientes, " + numOpsPorCliente
                + " operaciones/cliente, servidor en " + HOST_SERVIDOR + ":" + PUERTO_SERVIDOR);
        ExecutorService poolClientes = Executors.newFixedThreadPool(numClientes);
        for (int i = 0; i < numClientes; i++) {
            poolClientes.execute(new TareaCliente(i + 1, numOpsPorCliente));
        }
        poolClientes.shutdown();
        try {
            if (!poolClientes.awaitTermination(5, TimeUnit.MINUTES)) {
                System.err.println("Algunas tareas de cliente no terminaron, forzando cierre.");
                poolClientes.shutdownNow();
            }
        } catch (InterruptedException e) {
            System.err.println("Simulación de clientes interrumpida.");
            poolClientes.shutdownNow();
            Thread.currentThread().interrupt();
        }
        System.out.println("Simulación de todos los clientes completada.");
    }

    private static void enviarYRecibir(Solicitud solicitud, ObjectOutputStream oos, ObjectInputStream ois,
            String idClienteLog)
            throws IOException, ClassNotFoundException {
        System.out.println("Cliente [" + idClienteLog + "]: Enviando: " + solicitud.getTipoOperacion() + " Params: "
                + solicitud.getParametros());
        oos.writeObject(solicitud);
        oos.flush();
        Object respuestaObj = ois.readObject();
        if (respuestaObj instanceof Respuesta) {
            Respuesta respuesta = (Respuesta) respuestaObj;
            System.out.println("Cliente [" + idClienteLog + "]: Respuesta Servidor: [" + respuesta.getEstado() + "] "
                    + respuesta.getMensaje() + (respuesta.getDatos() != null ? " Datos: " + respuesta.getDatos() : ""));
        } else {
            System.err.println("Cliente [" + idClienteLog + "]: Respuesta no es de tipo Respuesta: "
                    + (respuestaObj != null ? respuestaObj.getClass().getName() : "null"));
        }
    }
}
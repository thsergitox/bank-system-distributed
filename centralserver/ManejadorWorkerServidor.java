package centralserver;

import commons.InfoWorker;
import commons.MensajeWorker;
import java.io.BufferedInputStream;
import java.io.EOFException;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

// Asumiendo que esta clase está en el mismo 'paquete por defecto' que ServidorCentral

public class ManejadorWorkerServidor implements Runnable {
    private Socket socketWorker;
    private ObjectOutputStream oos;
    private ObjectInputStream ois;
    private BufferedInputStream bufferedInput; // For JSON protocol
    private byte[] protocolDetectionHeader; // Store the header read during protocol detection
    private String workerId;
    private ServidorCentral servidorCentral; // Aunque los métodos de registro sean static, puede ser útil para otras
                                             // cosas
    private boolean activo;
    private InfoWorker infoEsteWorker; // Para mantener la info del worker que este hilo maneja
    private boolean isJsonProtocol = false; // Detect protocol type

    public ManejadorWorkerServidor(Socket socketWorker, ServidorCentral servidorCentralInstance) {
        this.socketWorker = socketWorker;
        this.servidorCentral = servidorCentralInstance; // Guardar referencia (aunque usemos métodos estáticos de
                                                        // ServidorCentral para registro)
        this.activo = true;
        try {
            // DON'T create ObjectOutputStream yet - it writes a header immediately
            // Detect protocol type first
            detectProtocolAndCreateStreams();
            
            // Create appropriate streams based on protocol
            if (!isJsonProtocol) {
                // For Java protocol, create ObjectOutputStream
                this.oos = new ObjectOutputStream(socketWorker.getOutputStream());
            }
            
            String protocolType = isJsonProtocol ? "JSON (Python)" : "Java";
            System.out.println("ManejadorWorker: Streams creados para worker " + socketWorker.getInetAddress() + " [Protocol: " + protocolType + "]");
        } catch (IOException e) {
            System.err.println("ManejadorWorker: Error al crear streams para worker " + socketWorker.getInetAddress()
                    + ": " + e.getMessage());
            activo = false;
            cerrarSocket();
        }
    }

    private void detectProtocolAndCreateStreams() throws IOException {
        InputStream inputStream = socketWorker.getInputStream();
        this.bufferedInput = new BufferedInputStream(inputStream);
        
        // Read first 8 bytes to detect protocol
        byte[] header = new byte[8];
        int bytesRead = bufferedInput.read(header);
        
        if (bytesRead >= 8) {
            // Check if it's JSON protocol (length header + '{')
            int length = ByteBuffer.wrap(header, 0, 4).getInt();
            if (length > 0 && length < 10000 && header[4] == '{') {
                // This is JSON protocol from Python worker
                isJsonProtocol = true;
                this.protocolDetectionHeader = header; // Store the header for later use
                System.out.println("ManejadorWorker: Detected Python worker (JSON protocol), message length: " + length);
            }
        }
        
        if (!isJsonProtocol) {
            // For Java protocol, we need to put back the bytes we read and create ObjectInputStream
            // Create a new stream that includes the header we read
            InputStream combinedStream = new java.io.SequenceInputStream(
                new java.io.ByteArrayInputStream(header, 0, bytesRead),
                bufferedInput
            );
            this.ois = new ObjectInputStream(combinedStream);
        }
        // For JSON protocol, we'll use the stored header + bufferedInput
    }

    @Override
    public void run() {
        if (!activo) {
            return;
        }
        String remoteAddress = socketWorker.getInetAddress().getHostAddress();
        System.out.println("ManejadorWorker: Hilo iniciado para worker: " + remoteAddress);

        try {
            if (isJsonProtocol) {
                handlePythonWorker();
            } else {
                handleJavaWorker();
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
        } catch (Exception e) {
            System.err.println("ManejadorWorker: Error con worker "
                    + (workerId != null ? workerId : remoteAddress) + ": " + e.getMessage());
            if (this.infoEsteWorker != null)
                ServidorCentral.desregistrarWorker(this.workerId);
        } finally {
            if (this.infoEsteWorker == null && this.workerId != null) {
                // Cleanup if needed
            }
            cerrarRecursos();
        }
    }

    private void handlePythonWorker() throws IOException {
        String remoteAddress = socketWorker.getInetAddress().getHostAddress();
        
        // 1. Receive JSON registration message
        String jsonRegistration = receiveJsonMessage();
        if (jsonRegistration == null) {
            System.err.println("ManejadorWorker [" + remoteAddress + "]: Failed to receive JSON registration");
            return;
        }
        
        System.out.println("ManejadorWorker: Received JSON registration from Python worker: " + jsonRegistration);
        
        // Parse basic info from JSON (simplified parsing)
        this.workerId = extractWorkerId(jsonRegistration);
        int puertoTareasWorker = extractWorkerPort(jsonRegistration);
        
        if (workerId == null) {
            System.err.println("ManejadorWorker [" + remoteAddress + "]: Could not extract workerId from JSON");
            return;
        }
        
        System.out.println("ManejadorWorker: Python worker " + this.workerId + " registered on port " + puertoTareasWorker);
        
        // 2. Assign partitions
        List<String> particionesAsignadasAlWorker = new ArrayList<>();
        Map<String, List<? extends Serializable>> datosParaWorker = new HashMap<>();
        
        ServidorCentral.asignarParticionesAWorker(this.workerId, particionesAsignadasAlWorker, datosParaWorker);
        
        if (particionesAsignadasAlWorker.isEmpty()) {
            System.out.println("ManejadorWorker: No hay particiones disponibles para asignar al worker " + this.workerId);
            String errorResponse = createJsonResponse("ERROR", "No hay particiones para asignar.");
            sendJsonMessage(errorResponse);
            return;
        }
        
        // 3. Send partition assignment as JSON
        String jsonResponse = createJsonPartitionAssignment(particionesAsignadasAlWorker, datosParaWorker);
        sendJsonMessage(jsonResponse);
        System.out.println("ManejadorWorker: Sent partition data to Python worker " + this.workerId);
        
        // 4. Wait for confirmation
        String confirmationJson = receiveJsonMessage();
        if (confirmationJson != null && confirmationJson.contains("DATOS_RECIBIDOS_POR_WORKER")) {
            System.out.println("ManejadorWorker: Python worker " + this.workerId + " confirmed data reception");
            
            // 5. Register worker and send final confirmation
            this.infoEsteWorker = new InfoWorker(this.workerId, remoteAddress, puertoTareasWorker,
                    particionesAsignadasAlWorker, this.socketWorker);
            ServidorCentral.registrarActualizarWorker(this.infoEsteWorker, particionesAsignadasAlWorker);
            
            String finalConfirmation = createJsonResponse("CONFIRMACION_REGISTRO_COMPLETO", "Registro completo y particiones asignadas.");
            sendJsonMessage(finalConfirmation);
            System.out.println("ManejadorWorker: Registration completed for Python worker " + this.workerId);
        } else {
            System.err.println("ManejadorWorker [" + this.workerId + "]: No confirmation received from Python worker");
            ServidorCentral.removerParticionesDeWorker(this.workerId, particionesAsignadasAlWorker);
        }
    }

    private void handleJavaWorker() throws IOException, ClassNotFoundException {
        String remoteAddress = socketWorker.getInetAddress().getHostAddress();
        
        // Original Java worker handling logic
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

        // 2. Lógica de asignación de particiones
        List<String> particionesAsignadasAlWorker = new ArrayList<>();
        Map<String, List<? extends Serializable>> datosParaWorker = new HashMap<>();

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
            cerrarRecursos();
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

        } else {
            System.err.println("ManejadorWorker [" + this.workerId
                    + "]: Se esperaba DATOS_RECIBIDOS_POR_WORKER pero fue " + msgConfirmacionDatos.getTipo());
            ServidorCentral.removerParticionesDeWorker(this.workerId, particionesAsignadasAlWorker);
            cerrarRecursos();
            return;
        }
    }

    // JSON utility methods
    private String receiveJsonMessage() throws IOException {
        // For the first message, use the header we read during protocol detection
        if (protocolDetectionHeader != null) {
            // Use the stored header from protocol detection
            int length = ByteBuffer.wrap(protocolDetectionHeader, 0, 4).getInt();
            System.out.println("ManejadorWorker: Using stored header, JSON message length: " + length);
            
            // Calculate how much JSON data we already have from protocol detection
            int headerJsonBytes = Math.min(protocolDetectionHeader.length - 4, length);
            byte[] jsonBytes = new byte[length];
            
            // Copy the JSON data we already have from the header
            if (headerJsonBytes > 0) {
                System.arraycopy(protocolDetectionHeader, 4, jsonBytes, 0, headerJsonBytes);
            }
            
            // Read the remaining JSON data from the stream
            int totalRead = headerJsonBytes;
            while (totalRead < length) {
                int read = bufferedInput.read(jsonBytes, totalRead, length - totalRead);
                if (read == -1) {
                    System.err.println("ManejadorWorker: Unexpected end of stream while reading JSON");
                    return null;
                }
                totalRead += read;
            }
            
            // Clear the stored header so subsequent calls read normally
            protocolDetectionHeader = null;
            
            String jsonMessage = new String(jsonBytes, StandardCharsets.UTF_8);
            System.out.println("ManejadorWorker: Received JSON message: " + jsonMessage);
            return jsonMessage;
        } else {
            // For subsequent messages, read normally
            // Read length header
            byte[] lengthBytes = new byte[4];
            int bytesRead = bufferedInput.read(lengthBytes);
            if (bytesRead != 4) {
                System.err.println("ManejadorWorker: Failed to read JSON length header, got " + bytesRead + " bytes");
                return null;
            }
            
            int length = ByteBuffer.wrap(lengthBytes).getInt();
            if (length <= 0 || length > 100000) {
                System.err.println("ManejadorWorker: Invalid JSON message length: " + length);
                return null;
            }
            
            System.out.println("ManejadorWorker: Reading JSON message of length " + length);
            
            // Read JSON data
            byte[] jsonBytes = new byte[length];
            int totalRead = 0;
            while (totalRead < length) {
                int read = bufferedInput.read(jsonBytes, totalRead, length - totalRead);
                if (read == -1) {
                    System.err.println("ManejadorWorker: Unexpected end of stream while reading JSON");
                    return null;
                }
                totalRead += read;
            }
            
            String jsonMessage = new String(jsonBytes, StandardCharsets.UTF_8);
            System.out.println("ManejadorWorker: Received JSON message: " + jsonMessage);
            return jsonMessage;
        }
    }
    
    private void sendJsonMessage(String json) throws IOException {
        System.out.println("ManejadorWorker: Sending JSON message: " + json.substring(0, Math.min(200, json.length())) + "...");
        byte[] jsonBytes = json.getBytes(StandardCharsets.UTF_8);
        byte[] lengthBytes = ByteBuffer.allocate(4).putInt(jsonBytes.length).array();
        
        System.out.println("ManejadorWorker: Length bytes: " + java.util.Arrays.toString(lengthBytes) + " (length: " + jsonBytes.length + ")");
        
        // Send directly to socket output stream, NOT through ObjectOutputStream
        socketWorker.getOutputStream().write(lengthBytes);
        socketWorker.getOutputStream().write(jsonBytes);
        socketWorker.getOutputStream().flush();
        System.out.println("ManejadorWorker: JSON message sent successfully (length: " + jsonBytes.length + ")");
    }
    
    private String extractWorkerId(String json) {
        // Simple JSON parsing for workerId
        int start = json.indexOf("\"workerId\":");
        if (start == -1) return null;
        start = json.indexOf("\"", start + 11);
        if (start == -1) return null;
        int end = json.indexOf("\"", start + 1);
        if (end == -1) return null;
        return json.substring(start + 1, end);
    }
    
    private int extractWorkerPort(String json) {
        // Simple JSON parsing for puertoTareasWorker
        int start = json.indexOf("\"puertoTareasWorker\":");
        if (start == -1) return -1;
        start = json.indexOf(":", start) + 1;
        int end = json.indexOf(",", start);
        if (end == -1) end = json.indexOf("}", start);
        if (end == -1) return -1;
        try {
            return Integer.parseInt(json.substring(start, end).trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }
    
    private String createJsonResponse(String tipo, String mensaje) {
        return String.format("{\"tipo\":\"%s\",\"mensajeTexto\":\"%s\"}", tipo, mensaje);
    }
    
    private String createJsonPartitionAssignment(List<String> particiones, Map<String, List<? extends Serializable>> datos) {
        StringBuilder json = new StringBuilder();
        json.append("{\"tipo\":\"ASIGNACION_PARTICIONES_Y_DATOS\",");
        json.append("\"listaParticiones\":[");
        for (int i = 0; i < particiones.size(); i++) {
            if (i > 0) json.append(",");
            json.append("\"").append(particiones.get(i)).append("\"");
        }
        json.append("],\"datosPorParticion\":{");
        
        int count = 0;
        for (Map.Entry<String, List<? extends Serializable>> entry : datos.entrySet()) {
            if (count > 0) json.append(",");
            json.append("\"").append(entry.getKey()).append("\":");
            json.append(convertListToJson(entry.getValue()));
            count++;
        }
        
        json.append("},\"mensajeTexto\":\"Particiones asignadas por Java server\"}");
        return json.toString();
    }
    
    private String convertListToJson(List<? extends Serializable> list) {
        StringBuilder json = new StringBuilder("[");
        for (int i = 0; i < list.size(); i++) {
            if (i > 0) json.append(",");
            Object obj = list.get(i);
            
            if (obj instanceof commons.Cuenta) {
                commons.Cuenta cuenta = (commons.Cuenta) obj;
                json.append("{")
                    .append("\"idCuenta\":").append(cuenta.getIdCuenta()).append(",")
                    .append("\"idCliente\":").append(cuenta.getIdCliente()).append(",")
                    .append("\"saldo\":").append(cuenta.getSaldo()).append(",")
                    .append("\"tipoCuenta\":\"").append(cuenta.getTipoCuenta()).append("\"")
                    .append("}");
            } else if (obj instanceof commons.Cliente) {
                commons.Cliente cliente = (commons.Cliente) obj;
                json.append("{")
                    .append("\"idCliente\":").append(cliente.getIdCliente()).append(",")
                    .append("\"nombre\":\"").append(escapeJson(cliente.getNombre())).append("\",")
                    .append("\"email\":\"").append(escapeJson(cliente.getEmail())).append("\",")
                    .append("\"telefono\":\"").append(escapeJson(cliente.getTelefono())).append("\"")
                    .append("}");
            } else {
                // Fallback for unknown object types
                json.append("{}");
            }
        }
        json.append("]");
        return json.toString();
    }
    
    private String escapeJson(String str) {
        if (str == null) return "";
        return str.replace("\"", "\\\"").replace("\\", "\\\\").replace("\n", "\\n").replace("\r", "\\r");
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
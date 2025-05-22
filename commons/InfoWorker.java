package commons;

import java.io.Serializable;
import java.net.Socket;
import java.util.List;

public class InfoWorker implements Serializable {
    private static final long serialVersionUID = 7L;
    private String workerId;
    private String host;
    private int puertoTareas; // Puerto donde el worker escucha tareas del servidor central
    private List<String> particionesManejadas;
    private transient Socket socketConexionInicial; // Socket de la conexión de registro, no serializar para evitar
                                                    // problemas.
    // Usar transient y manejar su ciclo de vida si es para heartbeats.
    private long ultimoHeartbeat;
    private boolean activo;

    public InfoWorker(String workerId, String host, int puertoTareas, List<String> particiones,
            Socket socketConexionInicial) {
        this.workerId = workerId;
        this.host = host;
        this.puertoTareas = puertoTareas;
        this.particionesManejadas = particiones;
        this.socketConexionInicial = socketConexionInicial;
        this.ultimoHeartbeat = System.currentTimeMillis();
        this.activo = true;
    }

    // Getters
    public String getWorkerId() {
        return workerId;
    }

    public String getHost() {
        return host;
    }

    public int getPuertoTareas() {
        return puertoTareas;
    }

    public List<String> getParticionesManejadas() {
        return particionesManejadas;
    }

    public Socket getSocketConexionInicial() {
        return socketConexionInicial;
    }

    public long getUltimoHeartbeat() {
        return ultimoHeartbeat;
    }

    public boolean isActivo() {
        return activo;
    }

    // Setters
    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public void setPuertoTareas(int puertoTareas) {
        this.puertoTareas = puertoTareas;
    }

    public void setParticionesManejadas(List<String> particionesManejadas) {
        this.particionesManejadas = particionesManejadas;
    }

    public void setSocketConexionInicial(Socket socketConexionInicial) {
        this.socketConexionInicial = socketConexionInicial;
    }

    public void setUltimoHeartbeat(long ultimoHeartbeat) {
        this.ultimoHeartbeat = ultimoHeartbeat;
        this.activo = true; // Si recibe heartbeat, está activo
    }

    public void setActivo(boolean activo) {
        this.activo = activo;
    }

    @Override
    public String toString() {
        return "InfoWorker{" +
                "workerId='" + workerId + '\'' +
                ", host='" + host + '\'' +
                ", puertoTareas=" + puertoTareas +
                ", activo=" + activo +
                ", particionesManejadas=" + particionesManejadas +
                '}';
    }
}
package commons;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

public class MensajeWorker implements Serializable {
    private static final long serialVersionUID = 8L;

    public enum TipoMensaje {
        REGISTRO, ASIGNACION_PARTICIONES_Y_DATOS, DATOS_RECIBIDOS_POR_WORKER, CONFIRMACION_REGISTRO_COMPLETO,
        HEARTBEAT, RESPUESTA_HEARTBEAT,
        NUEVA_TAREA, RESULTADO_TAREA,
        ERROR
    }

    private TipoMensaje tipo;
    private String workerId;
    private List<String> listaParticiones; // Usado para REGISTRO (sugeridas/manejadas), ASIGNACION (asignadas)
    private Map<String, List<? extends Serializable>> datosPorParticion; // Servidor->Worker: Contenido para
                                                                         // ASIGNACION_PARTICIONES_Y_DATOS
    private Object datosAdicionalesTarea; // Para NUEVA_TAREA (contendrá Solicitud) o RESULTADO_TAREA (contendrá
                                          // Respuesta)
    private String mensajeTexto;
    private int puertoTareasWorker; // Worker->Servidor: En REGISTRO

    // Constructor vacío para flexibilidad o deserialización
    public MensajeWorker() {
    }

    // Worker -> Servidor: REGISTRO
    public MensajeWorker(String workerId, List<String> particionesSugeridas, int puertoTareasWorker) {
        this.tipo = TipoMensaje.REGISTRO;
        this.workerId = workerId;
        this.listaParticiones = particionesSugeridas;
        this.puertoTareasWorker = puertoTareasWorker;
        this.mensajeTexto = "Solicitud de registro";
    }

    // Servidor -> Worker: ASIGNACION_PARTICIONES_Y_DATOS
    public MensajeWorker(Map<String, List<? extends Serializable>> datosParticionesAsignadas,
            List<String> idsParticionesAsignadas, String mensaje) {
        this.tipo = TipoMensaje.ASIGNACION_PARTICIONES_Y_DATOS;
        this.datosPorParticion = datosParticionesAsignadas;
        this.listaParticiones = idsParticionesAsignadas; // Para que el worker sepa qué IDs de partición recibió
        this.mensajeTexto = mensaje;
    }

    // Worker -> Servidor: DATOS_RECIBIDOS_POR_WORKER
    public MensajeWorker(String workerId, String mensajeConfirmacionDatos) {
        this.tipo = TipoMensaje.DATOS_RECIBIDOS_POR_WORKER;
        this.workerId = workerId;
        this.mensajeTexto = mensajeConfirmacionDatos;
    }

    // Servidor -> Worker: CONFIRMACION_REGISTRO_COMPLETO (Después de que worker
    // confirma recepción de datos)
    public MensajeWorker(String workerIdConfirmado, String mensajeConfirmacion, boolean esConfirmacionFinal) {
        this.tipo = TipoMensaje.CONFIRMACION_REGISTRO_COMPLETO;
        this.workerId = workerIdConfirmado;
        this.mensajeTexto = mensajeConfirmacion;
    }

    // Para NUEVA_TAREA (Servidor -> Worker) o RESULTADO_TAREA (Worker -> Servidor)
    public MensajeWorker(TipoMensaje tipo, String remitenteOReceptorId, Object tareaOResultado) {
        if (tipo != TipoMensaje.NUEVA_TAREA && tipo != TipoMensaje.RESULTADO_TAREA) {
            throw new IllegalArgumentException(
                    "Constructor incorrecto para tipo: " + tipo + ". Usar para NUEVA_TAREA o RESULTADO_TAREA.");
        }
        this.tipo = tipo;
        this.workerId = remitenteOReceptorId; // ID del worker destino para NUEVA_TAREA, ID del worker origen para
                                              // RESULTADO_TAREA
        this.datosAdicionalesTarea = tareaOResultado;
    }

    // Para HEARTBEAT, RESPUESTA_HEARTBEAT, ERROR, o mensajes de texto simples
    public MensajeWorker(TipoMensaje tipo, String remitenteId, String mensajeTexto) {
        this.tipo = tipo;
        this.workerId = remitenteId;
        this.mensajeTexto = mensajeTexto;
    }

    // Getters
    public TipoMensaje getTipo() {
        return tipo;
    }

    public String getWorkerId() {
        return workerId;
    }

    public List<String> getListaParticiones() {
        return listaParticiones;
    }

    public Map<String, List<? extends Serializable>> getDatosPorParticion() {
        return datosPorParticion;
    }

    public Object getDatosAdicionalesTarea() {
        return datosAdicionalesTarea;
    }

    public String getMensajeTexto() {
        return mensajeTexto;
    }

    public int getPuertoTareasWorker() {
        return puertoTareasWorker;
    }

    // Setters (generalmente es mejor tener mensajes inmutables, pero se pueden
    // añadir si es necesario)
    // public void setTipo(TipoMensaje tipo) { this.tipo = tipo; }
    // ... otros setters

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("MensajeWorker{");
        sb.append("tipo=").append(tipo);
        if (workerId != null)
            sb.append(", workerId='").append(workerId).append('\'');
        if (listaParticiones != null)
            sb.append(", listaParticiones=").append(listaParticiones);
        if (datosPorParticion != null)
            sb.append(", datosPorParticion_count=").append(datosPorParticion.size());
        if (puertoTareasWorker > 0)
            sb.append(", puertoTareasWorker=").append(puertoTareasWorker);
        if (datosAdicionalesTarea != null)
            sb.append(", datosAdicionalesTarea_type=").append(datosAdicionalesTarea.getClass().getSimpleName());
        if (mensajeTexto != null)
            sb.append(", mensajeTexto='").append(mensajeTexto).append('\'');
        sb.append('}');
        return sb.toString();
    }
}
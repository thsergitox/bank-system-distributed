package communication;

import java.io.Serializable;

/**
 * Clase que representa un mensaje en el protocolo de comunicación
 * entre los componentes del sistema bancario distribuido.
 * Versión del Servidor Central.
 */
public class Message implements Serializable {
    private static final long serialVersionUID = 1L;

    // Tipos de operación soportados
    public enum OperationType {
        // Operaciones de cliente a servidor
        CONSULTAR_SALDO,
        TRANSFERIR_FONDOS,

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

        // Respuestas de servidor a cliente
        RESPUESTA_SALDO,
        RESPUESTA_TRANSFERENCIA,

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

    // Constructor para consulta de saldo (Cliente -> Servidor)
    public static Message consultarSaldo(Long idCuenta) {
        Message message = new Message();
        message.operationType = OperationType.CONSULTAR_SALDO;
        message.idCuenta = idCuenta;
        return message;
    }

    // Constructor para transferencia (Cliente -> Servidor)
    public static Message transferirFondos(Long origen, Long destino, Double monto) {
        Message message = new Message();
        message.operationType = OperationType.TRANSFERIR_FONDOS;
        message.idCuentaOrigen = origen;
        message.idCuentaDestino = destino;
        message.monto = monto;
        return message;
    }

    // Constructor para procesamiento de consulta (Servidor -> Nodo)
    public static Message procesarConsulta(Long idCuenta) {
        Message message = new Message();
        message.operationType = OperationType.PROCESAR_CONSULTA;
        message.idCuenta = idCuenta;
        return message;
    }

    // Constructor para verificación de saldo (Servidor -> Nodo)
    public static Message verificarSaldo(Long idCuenta, Double monto) {
        Message message = new Message();
        message.operationType = OperationType.VERIFICAR_SALDO;
        message.idCuenta = idCuenta;
        message.monto = monto;
        return message;
    }

    // Constructor para actualización de saldo (Servidor -> Nodo)
    public static Message actualizarSaldo(Long idCuenta, Double monto, boolean esIncremento) {
        Message message = new Message();
        message.operationType = OperationType.ACTUALIZAR_SALDO;
        message.idCuenta = idCuenta;
        message.monto = esIncremento ? monto : -monto;
        return message;
    }

    // Constructor para respuesta de saldo (Servidor -> Cliente)
    public static Message respuestaSaldo(Long idCuenta, Double saldo) {
        Message message = new Message();
        message.operationType = OperationType.RESPUESTA_SALDO;
        message.idCuenta = idCuenta;
        message.saldo = saldo;
        return message;
    }

    // Constructor para respuesta de transferencia (Servidor -> Cliente)
    public static Message respuestaTransferencia(boolean exitosa, String mensaje) {
        Message message = new Message();
        message.operationType = OperationType.RESPUESTA_TRANSFERENCIA;
        message.estado = exitosa ? "Confirmada" : "Rechazada";
        message.mensaje = mensaje;
        return message;
    }

    // Constructor para registro de nodo (Nodo -> Servidor)
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Message[");
        sb.append("type=").append(operationType);

        if (idCuenta != null)
            sb.append(", cuenta=").append(idCuenta);
        if (idCuentaOrigen != null)
            sb.append(", origen=").append(idCuentaOrigen);
        if (idCuentaDestino != null)
            sb.append(", destino=").append(idCuentaDestino);
        if (monto != null)
            sb.append(", monto=").append(monto);
        if (saldo != null)
            sb.append(", saldo=").append(saldo);
        if (mensaje != null)
            sb.append(", mensaje=").append(mensaje);
        if (estado != null)
            sb.append(", estado=").append(estado);
        if (nodoId != null)
            sb.append(", nodoId=").append(nodoId);

        sb.append("]");
        return sb.toString();
    }
}
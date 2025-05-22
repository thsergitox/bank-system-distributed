package commons;

import java.io.Serializable;
import java.util.Date; // Para FECHA_HORA

public class Transaccion implements Serializable {
    private static final long serialVersionUID = 3L;

    private int idTransaccion;
    private int idCuentaOrigen;
    private int idCuentaDestino;
    private double monto;
    private Date fechaHora; // Usamos Date para la fecha y hora
    private String estado; // Podría ser un Enum más adelante: Confirmada, Pendiente, Fallida

    public Transaccion() {
    }

    public Transaccion(int idTransaccion, int idCuentaOrigen, int idCuentaDestino, double monto, Date fechaHora,
            String estado) {
        this.idTransaccion = idTransaccion;
        this.idCuentaOrigen = idCuentaOrigen;
        this.idCuentaDestino = idCuentaDestino;
        this.monto = monto;
        this.fechaHora = fechaHora;
        this.estado = estado;
    }

    // Getters
    public int getIdTransaccion() {
        return idTransaccion;
    }

    public int getIdCuentaOrigen() {
        return idCuentaOrigen;
    }

    public int getIdCuentaDestino() {
        return idCuentaDestino;
    }

    public double getMonto() {
        return monto;
    }

    public Date getFechaHora() {
        return fechaHora;
    }

    public String getEstado() {
        return estado;
    }

    // Setters
    public void setIdTransaccion(int idTransaccion) {
        this.idTransaccion = idTransaccion;
    }

    public void setIdCuentaOrigen(int idCuentaOrigen) {
        this.idCuentaOrigen = idCuentaOrigen;
    }

    public void setIdCuentaDestino(int idCuentaDestino) {
        this.idCuentaDestino = idCuentaDestino;
    }

    public void setMonto(double monto) {
        this.monto = monto;
    }

    public void setFechaHora(Date fechaHora) {
        this.fechaHora = fechaHora;
    }

    public void setEstado(String estado) {
        this.estado = estado;
    }

    @Override
    public String toString() {
        return "Transaccion{" +
                "idTransaccion=" + idTransaccion +
                ", idCuentaOrigen=" + idCuentaOrigen +
                ", idCuentaDestino=" + idCuentaDestino +
                ", monto=" + monto +
                ", fechaHora=" + fechaHora +
                ", estado='" + estado + '\'' +
                '}';
    }
}
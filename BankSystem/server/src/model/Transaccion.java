package model;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * Clase que representa una transacción financiera en el sistema.
 */
public class Transaccion implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long id;
    private Long idCuentaOrigen;
    private Long idCuentaDestino;
    private Double monto;
    private LocalDateTime fechaHora;
    private String estado; // "Confirmada", "Pendiente", "Rechazada"

    public Transaccion() {
        this.fechaHora = LocalDateTime.now();
        this.estado = "Pendiente";
    }

    public Transaccion(Long id, Long idCuentaOrigen, Long idCuentaDestino, Double monto) {
        this.id = id;
        this.idCuentaOrigen = idCuentaOrigen;
        this.idCuentaDestino = idCuentaDestino;
        this.monto = monto;
        this.fechaHora = LocalDateTime.now();
        this.estado = "Pendiente";
    }

    /**
     * Confirma la transacción
     */
    public void confirmar() {
        this.estado = "Confirmada";
    }

    /**
     * Rechaza la transacción
     */
    public void rechazar() {
        this.estado = "Rechazada";
    }

    // Getters y setters

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
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

    public LocalDateTime getFechaHora() {
        return fechaHora;
    }

    public void setFechaHora(LocalDateTime fechaHora) {
        this.fechaHora = fechaHora;
    }

    public String getEstado() {
        return estado;
    }

    public void setEstado(String estado) {
        this.estado = estado;
    }

    @Override
    public String toString() {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        return "Transaccion{" +
                "id=" + id +
                ", idCuentaOrigen=" + idCuentaOrigen +
                ", idCuentaDestino=" + idCuentaDestino +
                ", monto=" + monto +
                ", fechaHora=" + fechaHora.format(formatter) +
                ", estado='" + estado + '\'' +
                '}';
    }
}
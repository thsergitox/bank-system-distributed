package model;

import java.io.Serializable;

/**
 * Clase que representa una cuenta bancaria en el sistema.
 */
public class Cuenta implements Serializable {
    private static final long serialVersionUID = 1L;

    private Long id;
    private Long idCliente;
    private Double saldo;
    private String tipoCuenta; // "Ahorros" o "Corriente"

    public Cuenta() {
    }

    public Cuenta(Long id, Long idCliente, Double saldo, String tipoCuenta) {
        this.id = id;
        this.idCliente = idCliente;
        this.saldo = saldo;
        this.tipoCuenta = tipoCuenta;
    }

    /**
     * Verifica si hay saldo suficiente para realizar una operación
     * 
     * @param monto El monto a verificar
     * @return true si hay saldo suficiente, false en caso contrario
     */
    public boolean tieneSaldoSuficiente(Double monto) {
        return saldo >= monto;
    }

    /**
     * Debita un monto de la cuenta
     * 
     * @param monto El monto a debitar
     * @return true si la operación fue exitosa, false en caso contrario
     */
    public boolean debitar(Double monto) {
        if (!tieneSaldoSuficiente(monto)) {
            return false;
        }

        saldo -= monto;
        return true;
    }

    /**
     * Acredita un monto a la cuenta
     * 
     * @param monto El monto a acreditar
     */
    public void acreditar(Double monto) {
        saldo += monto;
    }

    // Getters y setters

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Long getIdCliente() {
        return idCliente;
    }

    public void setIdCliente(Long idCliente) {
        this.idCliente = idCliente;
    }

    public Double getSaldo() {
        return saldo;
    }

    public void setSaldo(Double saldo) {
        this.saldo = saldo;
    }

    public String getTipoCuenta() {
        return tipoCuenta;
    }

    public void setTipoCuenta(String tipoCuenta) {
        this.tipoCuenta = tipoCuenta;
    }

    @Override
    public String toString() {
        return "Cuenta{" +
                "id=" + id +
                ", idCliente=" + idCliente +
                ", saldo=" + saldo +
                ", tipoCuenta='" + tipoCuenta + '\'' +
                '}';
    }
}
package commons;

import java.io.Serializable;

public class Cuenta implements Serializable {
    private static final long serialVersionUID = 2L;

    private int idCuenta;
    private int idCliente;
    private double saldo;
    private String tipoCuenta;

    public Cuenta() {
    }

    public Cuenta(int idCuenta, int idCliente, double saldo, String tipoCuenta) {
        this.idCuenta = idCuenta;
        this.idCliente = idCliente;
        this.saldo = saldo;
        this.tipoCuenta = tipoCuenta;
    }

    // Getters
    public int getIdCuenta() {
        return idCuenta;
    }

    public int getIdCliente() {
        return idCliente;
    }

    public double getSaldo() {
        return saldo;
    }

    public String getTipoCuenta() {
        return tipoCuenta;
    }

    // Setters
    public void setIdCuenta(int idCuenta) {
        this.idCuenta = idCuenta;
    }

    public void setIdCliente(int idCliente) {
        this.idCliente = idCliente;
    }

    public void setSaldo(double saldo) {
        this.saldo = saldo;
    }

    public void setTipoCuenta(String tipoCuenta) {
        this.tipoCuenta = tipoCuenta;
    }

    @Override
    public String toString() {
        return "Cuenta{" +
                "idCuenta=" + idCuenta +
                ", idCliente=" + idCliente +
                ", saldo=" + saldo +
                ", tipoCuenta='" + tipoCuenta + '\'' +
                '}';
    }
}
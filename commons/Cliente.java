package commons;

import java.io.Serializable;

public class Cliente implements Serializable {
    private static final long serialVersionUID = 1L; // Para control de versión de serialización

    private int idCliente;
    private String nombre;
    private String email;
    private String telefono;

    // Constructor vacío (puede ser útil para algunas librerías de serialización o
    // frameworks)
    public Cliente() {
    }

    // Constructor con todos los campos
    public Cliente(int idCliente, String nombre, String email, String telefono) {
        this.idCliente = idCliente;
        this.nombre = nombre;
        this.email = email;
        this.telefono = telefono;
    }

    // Getters
    public int getIdCliente() {
        return idCliente;
    }

    public String getNombre() {
        return nombre;
    }

    public String getEmail() {
        return email;
    }

    public String getTelefono() {
        return telefono;
    }

    // Setters
    public void setIdCliente(int idCliente) {
        this.idCliente = idCliente;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public void setTelefono(String telefono) {
        this.telefono = telefono;
    }

    @Override
    public String toString() {
        return "Cliente{" +
                "idCliente=" + idCliente +
                ", nombre='" + nombre + '\'' +
                ", email='" + email + '\'' +
                ", telefono='" + telefono + '\'' +
                '}';
    }
}
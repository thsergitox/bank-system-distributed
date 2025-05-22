package commons;

import java.io.Serializable;

public class Respuesta implements Serializable {
    private static final long serialVersionUID = 5L;

    private EstadoOperacion estado;
    private String mensaje;
    private Object datos; // Para devolver datos específicos de la operación, ej. el saldo

    public Respuesta() {
    }

    public Respuesta(EstadoOperacion estado, String mensaje, Object datos) {
        this.estado = estado;
        this.mensaje = mensaje;
        this.datos = datos;
    }

    public Respuesta(EstadoOperacion estado, String mensaje) {
        this(estado, mensaje, null);
    }

    // Getters
    public EstadoOperacion getEstado() {
        return estado;
    }

    public String getMensaje() {
        return mensaje;
    }

    public Object getDatos() {
        return datos;
    }

    // Setters
    public void setEstado(EstadoOperacion estado) {
        this.estado = estado;
    }

    public void setMensaje(String mensaje) {
        this.mensaje = mensaje;
    }

    public void setDatos(Object datos) {
        this.datos = datos;
    }

    @Override
    public String toString() {
        return "Respuesta{" +
                "estado=" + estado +
                ", mensaje='" + mensaje + '\'' +
                ", datos=" + (datos != null ? datos.toString() : "null") +
                '}';
    }
}
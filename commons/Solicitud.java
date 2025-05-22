package commons;

import java.io.Serializable;
import java.util.Map; // Para los parámetros

public class Solicitud implements Serializable {
    private static final long serialVersionUID = 4L;

    private TipoOperacion tipoOperacion;
    private Map<String, Object> parametros; // Usamos un Map para flexibilidad en los parámetros

    public Solicitud() {
    }

    public Solicitud(TipoOperacion tipoOperacion, Map<String, Object> parametros) {
        this.tipoOperacion = tipoOperacion;
        this.parametros = parametros;
    }

    // Getters
    public TipoOperacion getTipoOperacion() {
        return tipoOperacion;
    }

    public Map<String, Object> getParametros() {
        return parametros;
    }

    // Setters
    public void setTipoOperacion(TipoOperacion tipoOperacion) {
        this.tipoOperacion = tipoOperacion;
    }

    public void setParametros(Map<String, Object> parametros) {
        this.parametros = parametros;
    }

    @Override
    public String toString() {
        return "Solicitud{" +
                "tipoOperacion=" + tipoOperacion +
                ", parametros=" + parametros +
                '}';
    }
}
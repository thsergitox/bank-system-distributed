package commons;

public enum EstadoOperacion {
    EXITO,
    ERROR_SALDO_INSUFICIENTE,
    ERROR_CUENTA_ORIGEN_NO_EXISTE,
    ERROR_CUENTA_DESTINO_NO_EXISTE,
    ERROR_REINTENTAR_EN_OTRO_NODO, // Para que el Servidor Central sepa que debe reintentar
    ERROR_GENERAL_SERVIDOR, // Error no específico en el servidor o worker
    ERROR_COMUNICACION, // Error en la comunicación de red
    TRANSACCION_PENDIENTE, // Para transacciones que podrían tomar más tiempo o están en espera
    TRANSACCION_CONFIRMADA,
    TRANSACCION_FALLIDA,

    // Estados para transacciones distribuidas
    DEBITO_PREPARADO_OK, // Worker Origen: Saldo validado, monto retenido/debitado provisionalmente
    DEBITO_CONFIRMADO_OK, // Worker Origen: Débito finalizado
    DEBITO_REVERTIDO_OK, // Worker Origen: Débito provisional revertido
    CREDITO_APLICADO_OK, // Worker Destino: Crédito aplicado
    REPLICA_ACTUALIZADA_OK // Worker Réplica: Actualización de saldo aplicada
}
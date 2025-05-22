package commons;

public enum TipoOperacion {
    CONSULTAR_SALDO,
    TRANSFERIR_FONDOS, // Operación principal iniciada por el cliente

    // Sub-operaciones para transacciones distribuidas (Servidor -> Worker)
    PREPARAR_DEBITO, // Validar saldo y "retener" fondos en cuenta origen
    CONFIRMAR_DEBITO, // Confirmar el débito que fue preparado
    REVERTIR_DEBITO, // Cancelar un débito preparado
    APLICAR_CREDITO, // Aplicar crédito en cuenta destino
    // REVERTIR_CREDITO (Podría ser necesario si la confirmación del débito falla
    // después de un crédito exitoso)

    // Operación para replicación (Servidor -> Worker)
    ACTUALIZAR_SALDO_REPLICA, // Instruye a un worker (que es réplica) a actualizar un saldo

    // Podríamos añadir más tipos de operaciones en el futuro si es necesario
    // Por ejemplo: CREAR_CUENTA, ACTUALIZAR_CLIENTE, ARQUEO_TOTAL

    // ARQUEO_CUENTAS, // Cliente -> Servidor: Solicita el arqueo total de todas las
    // cuentas
    // CALCULAR_SALDO_PARTICION // Servidor -> Worker: Solicita la suma de saldos de
    // una partición específica
}
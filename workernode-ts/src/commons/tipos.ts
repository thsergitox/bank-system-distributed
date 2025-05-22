export enum TipoOperacion {
    CONSULTAR_SALDO = "CONSULTAR_SALDO",
    TRANSFERIR_FONDOS = "TRANSFERIR_FONDOS",
    PREPARAR_DEBITO = "PREPARAR_DEBITO",
    APLICAR_CREDITO = "APLICAR_CREDITO",
    CONFIRMAR_DEBITO = "CONFIRMAR_DEBITO",
    REVERTIR_DEBITO = "REVERTIR_DEBITO",
    ACTUALIZAR_SALDO_REPLICA = "ACTUALIZAR_SALDO_REPLICA",
    // Agrega otros tipos de operación según sea necesario
}

export enum EstadoOperacion {
    EXITO = "EXITO",
    ERROR_SALDO_INSUFICIENTE = "ERROR_SALDO_INSUFICIENTE",
    ERROR_CUENTA_NO_ENCONTRADA = "ERROR_CUENTA_NO_ENCONTRADA",
    ERROR_CUENTA_ORIGEN_NO_EXISTE = "ERROR_CUENTA_ORIGEN_NO_EXISTE",
    ERROR_CUENTA_DESTINO_NO_EXISTE = "ERROR_CUENTA_DESTINO_NO_EXISTE",
    ERROR_COMUNICACION = "ERROR_COMUNICACION",
    ERROR_GENERAL_SERVIDOR = "ERROR_GENERAL_SERVIDOR",
    DEBITO_PREPARADO_OK = "DEBITO_PREPARADO_OK",
    CREDITO_APLICADO_OK = "CREDITO_APLICADO_OK",
    DEBITO_CONFIRMADO_OK = "DEBITO_CONFIRMADO_OK",
    DEBITO_REVERTIDO_OK = "DEBITO_REVERTIDO_OK",
    REPLICA_ACTUALIZADA_OK = "REPLICA_ACTUALIZADA_OK",
    // Agrega otros estados según sea necesario
}

export interface Solicitud {
    tipoOperacion: TipoOperacion;
    parametros: Record<string, any>;
}

export interface Respuesta {
    estado: EstadoOperacion;
    mensaje: string;
    datos?: any;
}

export interface Cliente {
    idCliente: number;
    nombre: string;
    email: string;
    telefono: string;
}

export interface Cuenta {
    idCuenta: number;
    idCliente: number;
    saldo: number;
    tipoCuenta: string; // Ej: "AHORRO", "CORRIENTE"
}

export enum TipoMensajeWorker {
    REGISTRO = "REGISTRO",
    ASIGNACION_PARTICIONES_Y_DATOS = "ASIGNACION_PARTICIONES_Y_DATOS",
    DATOS_RECIBIDOS_POR_WORKER = "DATOS_RECIBIDOS_POR_WORKER",
    CONFIRMACION_REGISTRO_COMPLETO = "CONFIRMACION_REGISTRO_COMPLETO",
    ERROR = "ERROR",
    // ...otros tipos de mensajes
}

export interface MensajeWorkerBase {
    tipo: TipoMensajeWorker;
    workerId?: string;
    mensajeTexto?: string;
}

export interface MensajeRegistroWorker extends MensajeWorkerBase {
    tipo: TipoMensajeWorker.REGISTRO;
    workerId: string;
    puertoTareasWorker: number;
    // particionesSugeridas?: string[]; // Si es necesario
}

export interface MensajeAsignacionParticiones extends MensajeWorkerBase {
    tipo: TipoMensajeWorker.ASIGNACION_PARTICIONES_Y_DATOS;
    listaParticiones: string[];
    datosPorParticion: Record<string, (Cliente | Cuenta)[]>; // Ejemplo
}

export interface MensajeConfirmacionDatosWorker extends MensajeWorkerBase {
    tipo: TipoMensajeWorker.DATOS_RECIBIDOS_POR_WORKER;
    workerId: string;
}

export interface MensajeConfirmacionRegistro extends MensajeWorkerBase {
    tipo: TipoMensajeWorker.CONFIRMACION_REGISTRO_COMPLETO;
}


export interface MensajeErrorWorker extends MensajeWorkerBase {
    tipo: TipoMensajeWorker.ERROR;
    // mensajeTexto y workerId son heredados de MensajeWorkerBase
}

// Union type para todos los mensajes de worker
export type MensajeWorker =
    | MensajeRegistroWorker
    | MensajeAsignacionParticiones
    | MensajeConfirmacionDatosWorker
    | MensajeConfirmacionRegistro
    | MensajeErrorWorker; // Añadir MensajeErrorWorker aquí
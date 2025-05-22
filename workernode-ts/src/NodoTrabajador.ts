import * as net from 'net';
import * as fs from 'fs';
import * as path from 'path';
import { Solicitud, Respuesta, EstadoOperacion, TipoOperacion, MensajeWorker, TipoMensajeWorker, MensajeAsignacionParticiones, MensajeRegistroWorker, MensajeConfirmacionDatosWorker, MensajeConfirmacionRegistro, Cliente, Cuenta } from './commons/tipos';
import { ManejadorTareaWorker } from './ManejadorTareaWorker';

const MAX_TAREAS_CONCURRENTES = 10; // Similar al ExecutorService

export class NodoTrabajador {
    public workerId: string;
    private hostServidorCentral: string;
    private puertoServidorCentral: number;
    private puertoEscuchaTareas: number;
    private particionesAsignadasLocalmente: string[] = [];
    private directorioBaseDatos: string;
    private archivoLogTransaccionesLocal: string;
    private registradoYDatosCargados = false;
    private activeConnections = 0;

    constructor(
        workerId: string,
        hostServidorCentral: string,
        puertoServidorCentral: number,
        puertoEscuchaTareas: number
    ) {
        this.workerId = workerId;
        this.hostServidorCentral = hostServidorCentral;
        this.puertoServidorCentral = puertoServidorCentral;
        this.puertoEscuchaTareas = puertoEscuchaTareas;
        this.directorioBaseDatos = `data_${this.workerId}`;
        this.archivoLogTransaccionesLocal = path.join(this.directorioBaseDatos, 'transacciones_locales.log');

        if (!fs.existsSync(this.directorioBaseDatos)) {
            fs.mkdirSync(this.directorioBaseDatos, { recursive: true });
        }
    }

    public async iniciar(): Promise<void> {
        try {
            this.registradoYDatosCargados = await this.registrarYRecibirDatos();
            if (this.registradoYDatosCargados) {
                console.log(`Worker [${this.workerId}]: Registrado y datos de partición recibidos. Directorio: ${this.directorioBaseDatos}.`);
                this.escucharTareas();
            } else {
                console.error(`Worker [${this.workerId}]: Proceso de registro y carga de datos fallido. Abortando.`);
                process.exit(1);
            }
        } catch (error) {
            console.error(`Worker [${this.workerId}]: Error al iniciar:`, error);
            process.exit(1);
        }
    }

    private registrarYRecibirDatos(): Promise<boolean> {
        return new Promise((resolve, reject) => {
            const socket = new net.Socket();
            let buffer = '';

            socket.connect(this.puertoServidorCentral, this.hostServidorCentral, () => {
                console.log(`Worker [${this.workerId}]: Conectado al Servidor Central para registro.`);
                const msgRegistro: MensajeRegistroWorker = {
                    tipo: TipoMensajeWorker.REGISTRO,
                    workerId: this.workerId,
                    puertoTareasWorker: this.puertoEscuchaTareas,
                };
                socket.write(JSON.stringify(msgRegistro) + '\n');
            });

            socket.on('data', (data) => {
                buffer += data.toString();
                let newlineIndex;
                while ((newlineIndex = buffer.indexOf('\n')) !== -1) {
                    const jsonData = buffer.substring(0, newlineIndex);
                    buffer = buffer.substring(newlineIndex + 1);
                    try {
                        const mensajeServidor = JSON.parse(jsonData) as MensajeWorker;
                        console.log(`Worker [${this.workerId}]: Mensaje recibido del servidor: ${mensajeServidor.tipo}`);

                        if (mensajeServidor.tipo === TipoMensajeWorker.ASIGNACION_PARTICIONES_Y_DATOS) {
                            const msgAsignacion = mensajeServidor as MensajeAsignacionParticiones;
                            if (this.guardarDatosDeParticiones(msgAsignacion.datosPorParticion, msgAsignacion.listaParticiones)) {
                                this.particionesAsignadasLocalmente.push(...msgAsignacion.listaParticiones);
                                const msgConfirmacion: MensajeConfirmacionDatosWorker = {
                                    tipo: TipoMensajeWorker.DATOS_RECIBIDOS_POR_WORKER,
                                    workerId: this.workerId,
                                    mensajeTexto: "Datos recibidos y guardados"
                                };
                                socket.write(JSON.stringify(msgConfirmacion) + '\n');
                            } else {
                                // Enviar error si falla el guardado
                                socket.write(JSON.stringify({ tipo: TipoMensajeWorker.ERROR, workerId: this.workerId, mensajeTexto: "Error al guardar datos" }) + '\n');
                                reject(new Error("Error al guardar datos de particiones"));
                                socket.end();
                            }
                        } else if (mensajeServidor.tipo === TipoMensajeWorker.CONFIRMACION_REGISTRO_COMPLETO) {
                            console.log(`Worker [${this.workerId}]: Registro completo confirmado por el servidor.`);
                            resolve(true);
                            socket.end();
                        } else if (mensajeServidor.tipo === TipoMensajeWorker.ERROR) {
                            console.error(`Worker [${this.workerId}]: Error del servidor durante el registro: ${mensajeServidor.mensajeTexto}`);
                            reject(new Error(mensajeServidor.mensajeTexto || "Error del servidor"));
                            socket.end();
                        }
                    } catch (e) {
                        console.error(`Worker [${this.workerId}]: Error al parsear JSON del servidor:`, e);
                        reject(e);
                        socket.end();
                    }
                }
            });

            socket.on('error', (err) => {
                console.error(`Worker [${this.workerId}]: Error de conexión con Servidor Central:`, err.message);
                reject(err);
            });

            socket.on('close', () => {
                console.log(`Worker [${this.workerId}]: Conexión de registro cerrada.`);
                 // Si no se resolvió antes, es un fallo
                if (!this.registradoYDatosCargados) {
                    reject(new Error("Conexión cerrada antes de completar el registro"));
                }
            });
        });
    }

    private guardarDatosDeParticiones(datosPorParticion: Record<string, any[]>, listaParticiones: string[]): boolean {
        console.log(`Worker [${this.workerId}]: Guardando datos de particiones: ${listaParticiones.join(', ')}`);
        try {
            for (const idParticion of listaParticiones) {
                const datos = datosPorParticion[idParticion];
                if (!datos) {
                    console.warn(`Worker [${this.workerId}]: No se encontraron datos para la partición ${idParticion}`);
                    continue;
                }
                const nombreArchivo = path.join(this.directorioBaseDatos, `${idParticion}.txt`);
                let contenidoArchivo = "";
                if (idParticion.startsWith("CUENTA_P")) {
                    contenidoArchivo = "ID_CUENTA|ID_CLIENTE|SALDO|TIPO_CUENTA\n";
                    contenidoArchivo += datos.map((c: Cuenta) => `${c.idCuenta}|${c.idCliente}|${c.saldo.toFixed(2)}|${c.tipoCuenta}`).join('\n');
                } else if (idParticion.startsWith("CLIENTE_P")) {
                    contenidoArchivo = "ID_CLIENTE|NOMBRE|EMAIL|TELEFONO\n";
                    contenidoArchivo += datos.map((c: Cliente) => `${c.idCliente}|${c.nombre}|${c.email}|${c.telefono}`).join('\n');
                }
                fs.writeFileSync(nombreArchivo, contenidoArchivo + '\n');
                console.log(`Worker [${this.workerId}]: Datos para partición ${idParticion} guardados en ${nombreArchivo}`);
            }
            return true;
        } catch (error) {
            console.error(`Worker [${this.workerId}]: Error al guardar datos de particiones:`, error);
            return false;
        }
    }

    private escucharTareas(): void {
        const server = net.createServer((socket) => {
            if (this.activeConnections >= MAX_TAREAS_CONCURRENTES) {
                console.warn(`Worker [${this.workerId}]: Máximo de conexiones alcanzado. Rechazando nueva conexión.`);
                socket.end(JSON.stringify({ estado: EstadoOperacion.ERROR_GENERAL_SERVIDOR, mensaje: "Worker ocupado, intente más tarde" }) + '\n');
                return;
            }
            this.activeConnections++;
            console.log(`Worker [${this.workerId}]: Nueva conexión de tarea desde ${socket.remoteAddress}:${socket.remotePort}. Conexiones activas: ${this.activeConnections}`);
            
            const manejador = new ManejadorTareaWorker(socket, this);
            manejador.handleConnection()
                .catch(err => console.error(`Worker [${this.workerId}]: Error en ManejadorTareaWorker:`, err))
                .finally(() => {
                    this.activeConnections--;
                    console.log(`Worker [${this.workerId}]: Conexión de tarea cerrada. Conexiones activas: ${this.activeConnections}`);
                });
        });

        server.on('error', (err) => {
            console.error(`Worker [${this.workerId}]: Error en el servidor de tareas:`, err);
        });

        server.listen(this.puertoEscuchaTareas, () => {
            console.log(`Worker [${this.workerId}]: Escuchando tareas del Servidor Central en puerto ${this.puertoEscuchaTareas}`);
        });
    }

    // Implementación simplificada de leerSaldoDeArchivo
    public async leerSaldoDeArchivo(idParticion: string, idCuentaBuscada: number): Promise<number | null> {
        const nombreArchivo = path.join(this.directorioBaseDatos, `${idParticion}.txt`);
        if (!fs.existsSync(nombreArchivo)) return null;

        const contenido = await fs.promises.readFile(nombreArchivo, 'utf-8');
        const lineas = contenido.split('\n');
        for (let i = 1; i < lineas.length; i++) { // Saltar cabecera
            const partes = lineas[i].split('|');
            if (partes.length >= 3 && parseInt(partes[0]) === idCuentaBuscada) {
                return parseFloat(partes[2]);
            }
        }
        return null;
    }
    
    // Debes implementar actualizarSaldosEnArchivo, actualizarSaldoUnicaCuentaEnArchivo, registrarTransaccionLocal
    // de forma similar, usando fs.promises para operaciones asíncronas y
    // manejando la concurrencia si es necesario (aunque Node.js es single-threaded,
    // las operaciones de I/O son asíncronas). El patrón de archivo temporal es bueno.

    public async procesarSolicitud(solicitud: Solicitud): Promise<Respuesta> {
        console.log(`Worker [${this.workerId}]: Procesando ${solicitud.tipoOperacion} params: ${JSON.stringify(solicitud.parametros)}`);
        const { ID_PARTICION: idParticionSolicitada, ID_CUENTA: idCuenta, MONTO: monto, ID_CUENTA_ORIGEN: idCuentaOrigen, ID_CUENTA_DESTINO: idCuentaDestino } = solicitud.parametros;

        if (idParticionSolicitada && !this.particionesAsignadasLocalmente.includes(idParticionSolicitada)) {
            return { estado: EstadoOperacion.ERROR_GENERAL_SERVIDOR, mensaje: `Worker no maneja la partición ${idParticionSolicitada}` };
        }

        switch (solicitud.tipoOperacion) {
            case TipoOperacion.CONSULTAR_SALDO:
                if (!idCuenta || !idParticionSolicitada) {
                    return { estado: EstadoOperacion.ERROR_GENERAL_SERVIDOR, mensaje: "Parámetros incompletos para CONSULTAR_SALDO" };
                }
                const saldo = await this.leerSaldoDeArchivo(idParticionSolicitada, parseInt(idCuenta));
                if (saldo !== null) {
                    return { estado: EstadoOperacion.EXITO, mensaje: `Saldo: ${saldo}`, datos: saldo };
                } else {
                    return { estado: EstadoOperacion.ERROR_CUENTA_NO_ENCONTRADA, mensaje: `Cuenta ${idCuenta} no encontrada en partición ${idParticionSolicitada}` };
                }
            // TODO: Implementar otros casos como TRANSFERIR_FONDOS, etc.
            // Estos requerirán lógica de lectura y escritura de archivos más compleja,
            // similar a actualizarSaldosEnArchivo y actualizarSaldoUnicaCuentaEnArchivo en Java.
            // Por ejemplo:
            // case TipoOperacion.TRANSFERIR_FONDOS:
            //     // ... lógica para transferir fondos ...
            //     // Necesitarás leer saldos, verificar, y luego actualizar archivos.
            //     // Considera usar el patrón de archivo temporal para escrituras atómicas.
            //     return { estado: EstadoOperacion.EXITO, mensaje: "Transferencia (simulada) OK" };

            default:
                return { estado: EstadoOperacion.ERROR_GENERAL_SERVIDOR, mensaje: `Operación ${solicitud.tipoOperacion} no soportada` };
        }
    }
}

// Punto de entrada principal
if (require.main === module) {
    if (process.argv.length < 6) {
        console.error("Uso: node dist/NodoTrabajador.js <workerId> <hostServidorCentral> <puertoServidorCentral> <puertoEscuchaTareas>");
        process.exit(1);
    }

    const [, , workerId, hostServidor, puertoServidorStr, puertoTareasStr] = process.argv;
    const puertoServidor = parseInt(puertoServidorStr);
    const puertoTareas = parseInt(puertoTareasStr);

    if (isNaN(puertoServidor) || isNaN(puertoTareas)) {
        console.error("Los puertos deben ser números válidos.");
        process.exit(1);
    }

    const worker = new NodoTrabajador(workerId, hostServidor, puertoServidor, puertoTareas);
    worker.iniciar().catch(err => {
        console.error(`Worker [${workerId}] FAILED TO START:`, err);
        process.exit(1);
    });
}
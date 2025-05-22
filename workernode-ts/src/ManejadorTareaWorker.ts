
import * as net from 'net';
import { NodoTrabajador } from './NodoTrabajador';
import { Solicitud, Respuesta, EstadoOperacion } from './commons/tipos';

export class ManejadorTareaWorker {
    private socket: net.Socket;
    private nodoTrabajador: NodoTrabajador;
    private buffer: string = '';

    constructor(socket: net.Socket, nodoTrabajador: NodoTrabajador) {
        this.socket = socket;
        this.nodoTrabajador = nodoTrabajador;
    }

    public async handleConnection(): Promise<void> {
        this.socket.on('data', async (data) => {
            this.buffer += data.toString();
            let newlineIndex;

            while ((newlineIndex = this.buffer.indexOf('\n')) !== -1) {
                const jsonData = this.buffer.substring(0, newlineIndex);
                this.buffer = this.buffer.substring(newlineIndex + 1);

                try {
                    const solicitud = JSON.parse(jsonData) as Solicitud;
                    console.log(`ManejadorTareaWorker [${this.nodoTrabajador.workerId}]: Solicitud recibida: ${solicitud.tipoOperacion}`);
                    
                    const respuesta = await this.nodoTrabajador.procesarSolicitud(solicitud);
                    
                    this.socket.write(JSON.stringify(respuesta) + '\n');
                    console.log(`ManejadorTareaWorker [${this.nodoTrabajador.workerId}]: Respuesta enviada: ${respuesta.estado}`);
                } catch (e) {
                    console.error(`ManejadorTareaWorker [${this.nodoTrabajador.workerId}]: Error al procesar solicitud o JSON:`, e);
                    const errorRespuesta: Respuesta = {
                        estado: EstadoOperacion.ERROR_COMUNICACION,
                        mensaje: "Error en formato de solicitud o procesamiento.",
                    };
                    this.socket.write(JSON.stringify(errorRespuesta) + '\n');
                }
            }
        });

        this.socket.on('error', (err) => {
            console.error(`ManejadorTareaWorker [${this.nodoTrabajador.workerId}]: Error en socket:`, err.message);
            // La conexión se cerrará automáticamente después de un error.
        });

        this.socket.on('close', () => {
            console.log(`ManejadorTareaWorker [${this.nodoTrabajador.workerId}]: Conexión cerrada por el cliente.`);
        });

        // Considera un timeout para la conexión si no hay actividad
        // this.socket.setTimeout(30000); // 30 segundos
        // this.socket.on('timeout', () => {
        //     console.log(`ManejadorTareaWorker [${this.nodoTrabajador.workerId}]: Socket timeout.`);
        //     this.socket.end();
        // });
    }
}
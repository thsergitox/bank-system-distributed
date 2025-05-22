"use strict";
var __createBinding = (this && this.__createBinding) || (Object.create ? (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    var desc = Object.getOwnPropertyDescriptor(m, k);
    if (!desc || ("get" in desc ? !m.__esModule : desc.writable || desc.configurable)) {
      desc = { enumerable: true, get: function() { return m[k]; } };
    }
    Object.defineProperty(o, k2, desc);
}) : (function(o, m, k, k2) {
    if (k2 === undefined) k2 = k;
    o[k2] = m[k];
}));
var __setModuleDefault = (this && this.__setModuleDefault) || (Object.create ? (function(o, v) {
    Object.defineProperty(o, "default", { enumerable: true, value: v });
}) : function(o, v) {
    o["default"] = v;
});
var __importStar = (this && this.__importStar) || (function () {
    var ownKeys = function(o) {
        ownKeys = Object.getOwnPropertyNames || function (o) {
            var ar = [];
            for (var k in o) if (Object.prototype.hasOwnProperty.call(o, k)) ar[ar.length] = k;
            return ar;
        };
        return ownKeys(o);
    };
    return function (mod) {
        if (mod && mod.__esModule) return mod;
        var result = {};
        if (mod != null) for (var k = ownKeys(mod), i = 0; i < k.length; i++) if (k[i] !== "default") __createBinding(result, mod, k[i]);
        __setModuleDefault(result, mod);
        return result;
    };
})();
Object.defineProperty(exports, "__esModule", { value: true });
const net = __importStar(require("net"));
const DEFAULT_SERVER_HOST = 'localhost';
const DEFAULT_SERVER_PORT = 8000; // Puerto para clientes del ServidorCentral
const DEFAULT_NUM_CLIENTS = 5;
const DEFAULT_NUM_TRANSACTIONS = 10;
const MAX_ACCOUNT_ID = 200; // Max account number to use (e.g. 100-199)
const BASE_ACCOUNT_ID = 100;
class ClienteSimulador {
    clientId;
    serverHost;
    serverPort;
    socket = null;
    messageBuffer = '';
    onResponseCallback = null;
    constructor(clientId, host, port) {
        this.clientId = clientId;
        this.serverHost = host;
        this.serverPort = port;
    }
    conectar() {
        return new Promise((resolve, reject) => {
            if (this.socket && !this.socket.destroyed) {
                console.log(`Cliente TS ${this.clientId}: Ya conectado.`);
                resolve();
                return;
            }
            this.socket = new net.Socket();
            console.log(`Cliente TS ${this.clientId}: Conectando a ${this.serverHost}:${this.serverPort}...`);
            this.socket.connect(this.serverPort, this.serverHost, () => {
                console.log(`Cliente TS ${this.clientId}: Conectado al servidor.`);
                resolve();
            });
            this.socket.on('data', (data) => {
                this.messageBuffer += data.toString('utf-8');
                this.procesarBuffer();
            });
            this.socket.on('close', () => {
                console.log(`Cliente TS ${this.clientId}: Conexión cerrada.`);
                this.socket = null;
            });
            this.socket.on('error', (err) => {
                console.error(`Cliente TS ${this.clientId}: Error de conexión: ${err.message}`);
                this.socket = null;
                reject(err);
            });
        });
    }
    procesarBuffer() {
        let newlineIndex;
        while ((newlineIndex = this.messageBuffer.indexOf('\n')) !== -1) {
            const jsonResponse = this.messageBuffer.substring(0, newlineIndex);
            this.messageBuffer = this.messageBuffer.substring(newlineIndex + 1);
            if (jsonResponse) {
                try {
                    const response = JSON.parse(jsonResponse);
                    // console.log(`Cliente TS ${this.clientId}: Respuesta recibida:`, response);
                    if (this.onResponseCallback) {
                        this.onResponseCallback(response);
                    }
                }
                catch (e) {
                    console.error(`Cliente TS ${this.clientId}: Error parseando JSON del servidor: ${e.message}. Raw: '${jsonResponse}'`);
                }
            }
        }
    }
    enviarMensaje(message) {
        return new Promise((resolve, reject) => {
            if (!this.socket || this.socket.destroyed) {
                reject(new Error(`Cliente TS ${this.clientId}: No conectado al servidor.`));
                return;
            }
            const jsonMessage = JSON.stringify(message) + '\n'; // Añadir delimitador newline
            // console.log(`Cliente TS ${this.clientId}: Enviando: ${jsonMessage.trim()}`);
            this.socket.write(jsonMessage, 'utf-8');
            this.onResponseCallback = (response) => {
                this.onResponseCallback = null; // Clear callback after use
                resolve(response);
            };
            // Timeout para la respuesta (opcional pero recomendado)
            setTimeout(() => {
                if (this.onResponseCallback) { // Si aún no se ha resuelto
                    this.onResponseCallback = null;
                    reject(new Error(`Cliente TS ${this.clientId}: Timeout esperando respuesta para ${message.operationType}`));
                }
            }, 5000); // Timeout de 5 segundos
        });
    }
    async consultarSaldo(idCuenta) {
        console.log(`Cliente TS ${this.clientId}: Solicitando saldo para cuenta ${idCuenta}`);
        try {
            const response = await this.enviarMensaje({
                operationType: "CONSULTAR_SALDO",
                idCuenta: String(idCuenta) // Enviar como string por consistencia con Java Client
            });
            console.log(`Cliente TS ${this.clientId}: Saldo cuenta ${idCuenta}: ${response.saldo !== undefined ? response.saldo : 'Error: ' + response.mensaje}`);
            return response;
        }
        catch (error) {
            console.error(`Cliente TS ${this.clientId}: Error consultando saldo para ${idCuenta}:`, error.message);
            return null;
        }
    }
    async transferir(origen, destino, monto) {
        console.log(`Cliente TS ${this.clientId}: Transfiriendo ${monto} de ${origen} a ${destino}`);
        try {
            const response = await this.enviarMensaje({
                operationType: "TRANSFERIR_FONDOS",
                idCuentaOrigen: String(origen),
                idCuentaDestino: String(destino),
                monto: monto
            });
            const exitosa = response.estado === "Confirmada";
            console.log(`Cliente TS ${this.clientId}: Transferencia ${origen} -> ${destino} por ${monto}: ${exitosa ? 'Éxitosa' : 'Fallida'} - ${response.mensaje}`);
            return response;
        }
        catch (error) {
            console.error(`Cliente TS ${this.clientId}: Error transfiriendo de ${origen} a ${destino}:`, error.message);
            return null;
        }
    }
    desconectar() {
        if (this.socket && !this.socket.destroyed) {
            this.socket.end(() => {
                console.log(`Cliente TS ${this.clientId}: Desconectado.`);
            });
            this.socket = null;
        }
    }
    async simularTransacciones(numTransacciones) {
        for (let j = 0; j < numTransacciones; j++) {
            try {
                const esConsulta = Math.random() < 0.5;
                if (esConsulta) {
                    const cuentaId = BASE_ACCOUNT_ID + Math.floor(Math.random() * MAX_ACCOUNT_ID);
                    await this.consultarSaldo(cuentaId);
                }
                else {
                    const origen = BASE_ACCOUNT_ID + Math.floor(Math.random() * MAX_ACCOUNT_ID);
                    let destino = BASE_ACCOUNT_ID + Math.floor(Math.random() * MAX_ACCOUNT_ID);
                    while (destino === origen) { // Asegurar que origen y destino sean diferentes
                        destino = BASE_ACCOUNT_ID + Math.floor(Math.random() * MAX_ACCOUNT_ID);
                    }
                    const monto = parseFloat((10.0 + Math.random() * 90.0).toFixed(2)); // Entre 10.00 y 100.00
                    await this.transferir(origen, destino, monto);
                }
                const delay = 100 + Math.floor(Math.random() * 1900); // Delay entre 100ms y 2s
                await new Promise(resolve => setTimeout(resolve, delay));
            }
            catch (e) {
                console.error(`Cliente TS ${this.clientId}: Error durante simulación (transacción ${j + 1}):`, e.message);
                // Decide if we should break or continue
                // if connection error, it might be good to break
                if (!this.socket || this.socket.destroyed) {
                    console.error(`Cliente TS ${this.clientId}: Conexión perdida, deteniendo simulación.`);
                    break;
                }
            }
        }
        console.log(`Cliente TS ${this.clientId}: Simulación completada (${numTransacciones} transacciones).`);
    }
}
async function main() {
    const args = process.argv.slice(2);
    const serverHost = args[0] || DEFAULT_SERVER_HOST;
    const serverPort = args[1] ? parseInt(args[1], 10) : DEFAULT_SERVER_PORT;
    const numClientes = args[2] ? parseInt(args[2], 10) : DEFAULT_NUM_CLIENTS;
    const numTransacciones = args[3] ? parseInt(args[3], 10) : DEFAULT_NUM_TRANSACTIONS;
    if (isNaN(serverPort) || isNaN(numClientes) || isNaN(numTransacciones)) {
        console.error("Uso: node dist/cliente.js [host] [puerto] [numClientes] [numTransaccionesPorCliente]");
        process.exit(1);
    }
    console.log(`Iniciando simulación con ${numClientes} clientes, ${numTransacciones} transacciones c/u.`);
    console.log(`Conectando a ${serverHost}:${serverPort}`);
    const promesasClientes = [];
    for (let i = 0; i < numClientes; i++) {
        const clienteId = i + 1;
        const prom = (async () => {
            const cliente = new ClienteSimulador(clienteId, serverHost, serverPort);
            try {
                await cliente.conectar();
                console.log(`Cliente TS ${clienteId}: Conectado, iniciando transacciones.`);
                await cliente.simularTransacciones(numTransacciones);
            }
            catch (error) {
                console.error(`Cliente TS ${clienteId}: Falló la simulación:`, error.message);
            }
            finally {
                cliente.desconectar();
                console.log(`Cliente TS ${clienteId}: Desconectado y finalizado.`);
            }
        })();
        promesasClientes.push(prom);
        // Pequeño delay para no saturar al servidor con conexiones simultáneas inmediatas
        if (numClientes > 1)
            await new Promise(r => setTimeout(r, 50));
    }
    await Promise.all(promesasClientes);
    console.log("Simulación de todos los clientes completada.");
}
main().catch(err => {
    console.error("Error fatal en la simulación:", err);
});

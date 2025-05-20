# Diagrama de Clases

## Visión General

El sistema está dividido en tres componentes principales: Cliente, Servidor Central y Nodos Trabajadores. Cada componente tiene sus propias clases que implementan su funcionalidad específica.

## Diagrama de Clases

```
+----------------+       +----------------+       +----------------+
|    Cliente     |       | ServidorCentral|       | NodoTrabajador |
+----------------+       +----------------+       +----------------+
| - servidor     |<----->| - serverSocket |<----->| - socket       |
| - puerto       |       | - threadPool   |       | - servidorCent.|
| - socket       |       | - mapaNodos    |       | - particiones  |
| - in/out       |       | - mapaReplicas |       | - threadPool   |
+----------------+       +----------------+       +----------------+
| + conectar()   |       | + iniciar()    |       | + iniciar()    |
| + desconectar()|       | + detener()    |       | + detener()    |
| + consultar()  |       | + registrarNodo|       | + cargarPart() |
| + transferir() |       | + asignarNodo()|       | + registrarNodo|
+----------------+       +----------------+       +----------------+
           |                     |                       |
           v                     v                       v
+----------------+       +----------------+       +----------------+
|    Message     |       | ManejadorClien.|       |   Particion    |
+----------------+       +----------------+       +----------------+
| - operationType|       | - socket       |       | - rutaArchivo  |
| - idCuenta     |       | - servidor     |       | - tipoTabla    |
| - origen/dest  |       | - in/out       |       | - registros    |
| - monto/saldo  |       | - conectado    |       +----------------+
| - mensaje      |       +----------------+       | + cargar()     |
+----------------+       | + run()        |       | + guardar()    |
| + consultarSaldo|       | + procesarMsj()|       | + buscarReg() |
| + transferirFond|       | + enviarMsj()  |       | + agregarReg()|
| + respuesta()  |       | + cerrarConex()|       | + actualizarReg|
+----------------+       +----------------+       +----------------+

+----------------+       +----------------+       +----------------+
|    Cliente     |       | Transaccion    |       |    Cuenta      |
+----------------+       +----------------+       +----------------+
| - id           |       | - id           |       | - id           |
| - nombre       |       | - origen       |       | - idCliente    |
| - email        |       | - destino      |       | - saldo        |
| - telefono     |       | - monto        |       | - tipoCuenta   |
+----------------+       | - fechaHora    |       +----------------+
| + getters      |       | - estado       |       | + getSaldo()   |
| + setters      |       +----------------+       | + setSaldo()   |
+----------------+       | + confirmar()  |       | + debitar()    |
                         | + rechazar()    |       | + acreditar()  |
                         +----------------+       +----------------+
```

## Descripción de Clases Principales

### Cliente
- **Responsabilidad**: Enviar solicitudes al servidor central y recibir respuestas
- **Atributos principales**:
  - servidor: Dirección IP o hostname del servidor
  - puerto: Puerto del servidor
  - socket: Socket de conexión con el servidor
- **Métodos principales**:
  - conectar(): Establece conexión con el servidor
  - consultarSaldo(): Envía una solicitud de saldo de una cuenta
  - transferir(): Envía una solicitud de transferencia entre cuentas

### ServidorCentral
- **Responsabilidad**: Coordinar las comunicaciones entre clientes y nodos
- **Atributos principales**:
  - puertoCliente: Puerto para conexiones de clientes
  - puertoNodo: Puerto para conexiones de nodos
  - poolClientes: Pool de hilos para atender clientes
  - poolNodos: Pool de hilos para atender nodos
  - nodos: Mapeo de nodos disponibles y sus particiones
- **Métodos principales**:
  - iniciar(): Inicia el servidor en dos hilos (clientes y nodos)
  - detener(): Detiene el servidor y libera recursos
  - registrarNodo(): Registra un nuevo nodo en el sistema

### NodoTrabajador
- **Responsabilidad**: Procesar operaciones sobre particiones de datos
- **Atributos principales**:
  - servidorCentral: Dirección del servidor central
  - puerto: Puerto del servidor central
  - nodoId: Identificador único del nodo
  - directorioParticiones: Directorio de almacenamiento
  - particiones: Mapa de particiones administradas
- **Métodos principales**:
  - iniciar(): Carga particiones y se conecta al servidor
  - cargarParticiones(): Carga datos de archivos
  - procesarConsulta(): Procesa una solicitud de consulta de saldo
  - actualizarSaldo(): Actualiza el saldo de una cuenta

### Message
- **Responsabilidad**: Encapsular los mensajes intercambiados entre componentes
- **Atributos principales**:
  - operationType: Tipo de operación (CONSULTAR_SALDO, TRANSFERIR_FONDOS, etc.)
  - idCuenta, idCuentaOrigen, idCuentaDestino: IDs de cuentas involucradas
  - monto, saldo: Valores monetarios
  - mensaje, estado: Mensajes y estados de la operación
- **Métodos principales**:
  - consultarSaldo(): Constructor para mensajes de consulta
  - transferirFondos(): Constructor para mensajes de transferencia
  - respuestaSaldo(): Constructor para respuestas de saldo

### ManejadorCliente / ManejadorNodo
- **Responsabilidad**: Procesar mensajes de clientes o nodos
- **Atributos principales**:
  - socket: Socket de conexión
  - servidor: Referencia al servidor central
  - entrada/salida: Streams para lectura/escritura
- **Métodos principales**:
  - run(): Método principal que ejecuta el hilo
  - procesarMensaje(): Procesa un mensaje recibido
  - enviarMensaje(): Envía un mensaje
  - cerrarConexion(): Cierra la conexión 
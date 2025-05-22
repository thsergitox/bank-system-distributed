# Plan de Implementación: Sistema Bancario Distribuido

## 1. Estructura del Proyecto
   - [x] Crear estructura de carpetas:
     - [x] `commons`
     - [x] `central-server`
     - [x] `worker-node`
     - [x] `client`
     - [x] `data` (para los CSV iniciales)
   - [x] Configurar archivos de build (Postergado, se usará compilación directa por ahora).

## 2. Módulo `commons`
   - [x] Definir clases de modelo: `Cliente.java`, `Cuenta.java`, `Transaccion.java`.
   - [x] Definir clases para mensajes: `Solicitud.java`, `Respuesta.java`, `MensajeWorker.java`, `InfoWorker.java`.
   - [x] Definir enumeraciones: `TipoOperacion.java`, `EstadoOperacion.java`.

## 3. Implementación del Servidor Central (`central-server`)
   - [x] Lógica de red para escuchar conexiones de clientes (`ServidorCentral`, `ManejadorClienteServidor`).
   - [x] Manejo concurrente de múltiples clientes (ExecutorService).
   - [x] Lógica para recibir solicitudes de los clientes (en `ManejadorClienteServidor`).
   - [x] **Mecanismo de Descubrimiento, Registro y Gestión de Datos para Nodos Trabajadores:**
     - [x] Lógica de red para escuchar conexiones de workers (`ServidorCentral`, `ManejadorWorkerServidor`).
     - [x] Al iniciar el Servidor Central:
       - [x] Leer `data/cuentas.csv` y `data/clientes.csv`.
       - [x] Definir estrategia de particionamiento (ej. N particiones por tabla basadas en rangos de ID).
       - [x] Poblar `cuentaAParticion` (y `clienteAParticion`) basado en esta estrategia.
     - [x] En `ManejadorWorkerServidor` (durante el registro del worker):
       - [x] Asignar un conjunto de IDs de partición al worker (considerando replicación triple).
       - [x] Enviar los datos correspondientes a cada partición asignada al worker.
       - [x] Worker crea sus archivos `.txt` locales. (Implementado en Worker)
       - [x] Worker confirma al servidor que está listo. (Flujo de mensajes implementado)
       - [x] Actualizar `workersActivos` (con `InfoWorker`) y `particionANodos`.
   - [x] Lógica para delegar operaciones a los Nodos Trabajadores (en `ManejadorClienteServidor`):
     - [x] Para `CONSULTAR_SALDO`:
       - [x] Identificar la partición y los nodos con réplicas.
       - [x] Seleccionar un nodo trabajador.
       - [x] Implementar comunicación real (socket) para enviar solicitud al worker y recibir respuesta.
     - [x] Para `TRANSFERIR_FONDOS`:
       - [x] Caso A (misma partición): Implementar delegación y lógica en el worker.
       - [x] Caso B (diferentes particiones): Implementar coordinación y posible compensación. (Lógica de coordinación en Servidor implementada)
       - [x] Manejo de replicación de escrituras. (Lógica básica de replicación en Servidor implementada)
   - [x] Manejo de fallos de Nodos Trabajadores (failover):
     - [x] Detectar si un nodo no responde (timeout, error de IO).
     - [x] Desregistrar worker y actualizar metadatos (`particionANodos`).
     - [ ] Reenviar la operación a otro nodo con una réplica (Parcialmente cubierto por la iteración en `procesarConsultaSaldo` y `procesarTransferenciaFondos`, podría mejorarse).
   - [x] Implementar loggeo de operaciones (`System.out.println`).
   - [x] Registro de Transacciones Globales.

## 4. Implementación de los Nodos Trabajadores (`worker-node`)
   - [x] Lógica de red para registrarse con el Servidor Central.
   - [x] Al recibir asignación de particiones y datos del Servidor Central:
     - [x] Crear/escribir los archivos `.txt` locales en su directorio `data_<workerId>/`.
     - [x] Enviar confirmación de preparación al Servidor Central.
   - [x] Lógica de red para escuchar tareas del Servidor Central (`NodoTrabajador`, `ManejadorTareaWorker`).
   - [x] Manejo concurrente de tareas (ExecutorService).
   - [x] Lógica para gestionar sus particiones de datos (archivos `.txt`):
     - [x] Cargar datos al iniciar (recibir del servidor y guardar).
     - [x] Leer datos para consultas (`leerSaldoDeArchivo`).
     - [x] Escribir/actualizar datos para transferencias (`actualizarSaldosEnArchivo`, `actualizarSaldoUnicaCuentaEnArchivo`).
   - [x] Implementar la lógica de las transacciones (en `NodoTrabajador.procesarSolicitud()`):
     - [x] `CONSULTAR_SALDO`: Verificar partición, leer saldo del archivo.
     - [x] Para `TRANSFERIR_FONDOS`:
       - [x] Validar saldo.
       - [x] Restar/Sumar monto en archivo (para Caso A y sub-operaciones de Caso B).
     - [x] Implementar sub-operaciones (`PREPARAR_DEBITO`, `APLICAR_CREDITO`, `CONFIRMAR_DEBITO`, `REVERTIR_DEBITO`).
     - [x] Implementar `ACTUALIZAR_SALDO_REPLICA`.
   - [x] Lógica para enviar resultados/confirmaciones al Servidor Central.
   - [x] Implementar loggeo de operaciones (`System.out.println`).
   - [x] Registro local de transacciones.

## 5. Implementación del Cliente (`client`)
   - [x] Lógica de red para conectarse al Servidor Central.
   - [x] Interfaz de usuario simple (consola). (Ahora es una simulación concurrente)
   - [x] Lógica para enviar solicitudes (`CONSULTAR_SALDO`, `TRANSFERIR_FONDOS`) y recibir respuestas.
   - [x] Simulación de múltiples clientes y transacciones concurrentes. (Implementado con ExecutorService y TareaCliente)
   - [x] Implementar loggeo de operaciones (`System.out.println`).

## 6. Pruebas y Despliegue

## Consideraciones Adicionales
   - **Sincronización y Concurrencia:** (Implementado en puntos críticos).
   - **Manejo de Errores:** (Implementado, con espacio para refinamiento).
   - **Configuración:** (Puertos y argumentos básicos definidos).
   - **Principios de Diseño:** (Intentando adherirse a KISS, DRY, SRP).
   - **Archivos .txt como BD:** (Implementado).
   - **Loggeo:** `System.out.println` (Implementado).

### Ejecución del Sistema

Para ejecutar el sistema distribuido de banca, siga estos pasos:

1. Ejecución del Servidor Central:
   - Abra una terminal y diríjase al directorio `centralserver`:
     ```bash
     cd centralserver
     ```
   - Compile el código (si aún no lo ha compilado):
     ```bash
     javac ServidorCentral.java
     ```
   - Ejecute el servidor:
     ```bash
     java ServidorCentral
     ```

2. Ejecución del Nodo Trabajador:
   - Abra otra terminal y diríjase al directorio `workernode`:
     ```bash
     cd workernode
     ```
   - Compile el código:
     ```bash
     javac NodoTrabajador.java
     ```
   - Ejecute un nodo trabajador especificando sus parámetros:
     ```bash
     java NodoTrabajador <workerId> <hostServidorCentral> <puertoServidorCentral> <puertoEscuchaTareas>
     ```
     Ejemplo:
     ```bash
     java NodoTrabajador worker1 localhost 12346 12350
     ```

3. Ejecución del Cliente:
   - Abra otra terminal y diríjase al directorio `client`:
     ```bash
     cd client
     ```
   - Compile el código:
     ```bash
     javac Cliente.java
     ```
   - Ejecute el cliente con las siguientes opciones:
     - Para modo simulación (por defecto):
       ```bash
       java Cliente -h localhost -p 12345 -c 5 -o 10
       ```
       (donde `-c` indica el número de clientes simulados y `-o` el número de operaciones por cliente).
     - Para modo interactivo:
       ```bash
       java Cliente -i
       ```

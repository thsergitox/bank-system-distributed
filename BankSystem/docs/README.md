# Sistema Bancario Distribuido

Este sistema implementa un banco distribuido que soporta consultas de saldo y transferencias entre cuentas, manteniendo la consistencia de los datos mediante la replicación de particiones entre múltiples nodos.

## Arquitectura General

El sistema se compone de 3 tipos de componentes:
1. **Clientes**: Generan solicitudes de transacciones
2. **Servidor Central**: Coordina todas las operaciones
3. **Nodos Trabajadores**: Almacenan y procesan datos

## Diagrama de Flujo de Transacciones

```
Cliente           Servidor Central           Nodo Trabajador
  |                     |                         |
  |--- Solicitud ------>|                         |
  |                     |--- Operación ---------->|
  |                     |                         |--- Procesa ---
  |                     |                         |<-- Resultado --
  |                     |<-- Respuesta ----------|
  |<-- Confirmación ----|                         |
```

## Explicación del Flujo Paso a Paso

### 1. Clientes
- Los clientes envían solicitudes de tipo CONSULTAR_SALDO o TRANSFERIR_FONDOS
- Cada cliente puede enviar múltiples solicitudes con delays aleatorios
- La comunicación es mediante sockets TCP hacia el servidor central

### 2. Servidor Central
- Recibe solicitudes de clientes mediante hilos (threads) concurrentes
- Determina qué nodo trabajador debe procesar la solicitud según:
  - Qué nodo tiene la partición de datos necesaria
  - Disponibilidad del nodo
- Si un nodo no responde o falla, el servidor central redirige la solicitud a otro nodo que tenga la réplica de los datos
- Espera la respuesta del nodo trabajador
- Envía confirmación o error al cliente que hizo la solicitud

### 3. Nodos Trabajadores
- Cada nodo almacena particiones específicas de las tablas
- Cada partición está replicada 3 veces en diferentes nodos
- Procesan operaciones usando hilos para aprovechar todos los núcleos del procesador
- Verifican condiciones (como saldo suficiente) antes de ejecutar operaciones
- Envían resultados al servidor central

## Ejemplo: CONSULTAR_SALDO
1. Cliente envía: CONSULTAR_SALDO(101)
2. Servidor Central determina qué nodo tiene la partición donde está la cuenta 101
3. Envía la solicitud al nodo correspondiente
4. El nodo busca la cuenta, obtiene el saldo y responde
5. Servidor Central reenvía el saldo al Cliente: "1500.00"

## Ejemplo: TRANSFERIR_FONDOS
1. Cliente envía: TRANSFERIR_FONDOS(101, 102, 500.00)
2. Servidor Central determina:
   - Qué nodo tiene la cuenta 101 (origen)
   - Qué nodo tiene la cuenta 102 (destino)
3. Envía solicitud al nodo con cuenta 101 para verificar saldo
4. Si hay saldo suficiente:
   - Actualiza saldo en cuenta 101
   - Envía solicitud al nodo con cuenta 102
   - Actualiza saldo en cuenta 102
   - Registra la transacción
5. Envía confirmación al Cliente

## Manejo de Particiones y Réplicas
Cada tabla se divide en particiones (parte1.1, parte1.2, etc.) y cada partición se replica en 3 nodos diferentes. Esto garantiza:
- Tolerancia a fallos: Si un nodo cae, los datos siguen disponibles en otros
- Balanceo de carga: Las operaciones pueden distribuirse entre nodos
- Alta disponibilidad: Los datos siempre están accesibles 
# Instrucciones de Ejecución

## Requisitos Previos
- Java Development Kit (JDK) 8 o superior
- Sistema operativo: Linux, Windows o macOS

## Estructura de Directorios
```
BankSystem/
├── client/src/        # Cliente que envía solicitudes
├── server/src/        # Servidor Central que coordina
│   ├── communication/ # Protocolos de comunicación
│   ├── model/         # Modelos de datos
│   └── service/       # Servicios del servidor
└── node/src/          # Nodos trabajadores que procesan datos
```

## Compilación del Sistema

### 1. Compilar el Servidor Central

```bash
# Desde la raíz del proyecto
mkdir -p BankSystem/server/bin
javac -d BankSystem/server/bin BankSystem/server/src/communication/*.java BankSystem/server/src/model/*.java BankSystem/server/src/service/*.java
```

### 2. Compilar el Cliente

```bash
# Desde la raíz del proyecto
mkdir -p BankSystem/client/bin
javac -d BankSystem/client/bin BankSystem/client/src/*.java
```

### 3. Compilar el Nodo Trabajador

```bash
# Desde la raíz del proyecto
mkdir -p BankSystem/node/bin
javac -d BankSystem/node/bin BankSystem/node/src/*.java
```

## Ejecución del Sistema

### 1. Iniciar el Servidor Central

```bash
# Desde la raíz del proyecto
cd BankSystem/server/bin
java service.ServidorCentral [puertoCliente] [puertoNodo] [maxThreadsClientes] [maxThreadsNodos]
```

Parámetros opcionales:
- puertoCliente: Puerto para conexiones de clientes (por defecto: 8000)
- puertoNodo: Puerto para conexiones de nodos (por defecto: 8001)
- maxThreadsClientes: Número máximo de hilos para atender clientes (por defecto: 100)
- maxThreadsNodos: Número máximo de hilos para atender nodos (por defecto: 20)

Ejemplo:
```bash
java service.ServidorCentral 8000 8001 50 10
```

### 2. Iniciar Nodos Trabajadores

Se deben iniciar múltiples nodos trabajadores para procesar las operaciones. Cada nodo debe tener un ID único y un directorio para almacenar sus particiones.

```bash
# Desde la raíz del proyecto
cd BankSystem/node/bin
java NodoTrabajador [servidorCentral] [puerto] [nodoId] [directorioParticiones] [numThreads]
```

Parámetros opcionales:
- servidorCentral: Dirección IP o hostname del servidor central (por defecto: localhost)
- puerto: Puerto del servidor central para nodos (por defecto: 8001)
- nodoId: Identificador único del nodo (por defecto: 1)
- directorioParticiones: Directorio donde se almacenan las particiones (por defecto: data/nodoX)
- numThreads: Número de hilos para procesar solicitudes (por defecto: número de núcleos disponibles)

Ejemplos para iniciar tres nodos:
```bash
# Nodo 1
java NodoTrabajador localhost 8001 1 ../../data/nodo1 4

# Nodo 2 (en otra terminal)
java NodoTrabajador localhost 8001 2 ../../data/nodo2 4

# Nodo 3 (en otra terminal)
java NodoTrabajador localhost 8001 3 ../../data/nodo3 4
```

### 3. Ejecutar Cliente

El cliente puede ejecutarse para simular múltiples conexiones y transacciones.

```bash
# Desde la raíz del proyecto
cd BankSystem/client/bin
java Cliente [servidor] [puerto] [numClientes] [numTransacciones]
```

Parámetros opcionales:
- servidor: Dirección IP o hostname del servidor central (por defecto: localhost)
- puerto: Puerto del servidor central para clientes (por defecto: 8000)
- numClientes: Número de clientes simultáneos a simular (por defecto: 10)
- numTransacciones: Número de transacciones por cliente (por defecto: 50)

Ejemplo:
```bash
java Cliente localhost 8000 5 20
```

## Notas Adicionales

- Asegúrese de iniciar el servidor central antes que los nodos trabajadores.
- Los nodos trabajadores crearán automáticamente particiones de ejemplo si no encuentran datos existentes.
- Para un mejor rendimiento, distribuya los nodos trabajadores en diferentes máquinas.
- Los logs se muestran en la consola para facilitar el seguimiento de operaciones. 
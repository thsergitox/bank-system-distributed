# Worker Node Python - Sistema Bancario Distribuido

Este directorio contiene la implementación en Python de un nodo trabajador compatible con el sistema bancario distribuido Java.

## Descripción

El Worker Node Python (`NodoTrabajadorPython`) es una implementación alternativa al worker Java que puede integrarse con el servidor central Java existente. Proporciona las mismas funcionalidades:

- Registro automático con el servidor central
- Recepción y almacenamiento de datos de partición
- Procesamiento de operaciones bancarias
- Manejo de transacciones distribuidas
- Replicación de datos

## Características

- **Compatible con servidor Java**: Comunicación via sockets con protocolo compatible
- **Operaciones soportadas**:
  - `CONSULTAR_SALDO`: Consulta el saldo de una cuenta
  - `TRANSFERIR_FONDOS`: Transferencias dentro de la misma partición
  - `PREPARAR_DEBITO`: Preparación de débitos para transacciones distribuidas
  - `CONFIRMAR_DEBITO`: Confirmación de débitos preparados
  - `REVERTIR_DEBITO`: Reversión de débitos en caso de fallo
  - `APLICAR_CREDITO`: Aplicación de créditos
  - `ACTUALIZAR_SALDO_REPLICA`: Actualización de réplicas
  - `CALCULAR_SALDO_PARTICION`: Cálculo de saldo total de partición (arqueo)

- **Gestión de archivos**: Almacenamiento local en archivos de texto plano
- **Logging**: Registro detallado de operaciones y transacciones
- **Concurrencia**: Manejo de múltiples tareas simultáneas

## Archivos

- `nodo_trabajador.py`: Implementación principal del worker
- `test_worker.py`: Suite de pruebas para validar funcionalidad
- `java_bridge.py`: Utilidades para comunicación Java-Python
- `requirements.txt`: Dependencias (solo librería estándar de Python)
- `README.md`: Este archivo

## Instalación y Uso

### Requisitos

- Python 3.7+
- Servidor central Java ejecutándose
- Datos CSV iniciales (`data/cuentas.csv`, `data/clientes.csv`)

### Instalación

```bash
# Clonar o copiar los archivos del worker Python
cd workernode-py

# No hay dependencias externas, solo Python estándar
```

### Ejecución

```bash
# Ejecutar worker Python
python nodo_trabajador.py <workerId> <hostServidorCentral> <puertoServidorCentral> <puertoEscuchaTareas>

# Ejemplo:
python nodo_trabajador.py worker_py1 localhost 12346 12351
```

### Parámetros

- `workerId`: Identificador único del worker (ej: worker_py1, worker_py2)
- `hostServidorCentral`: Host del servidor central (generalmente localhost)
- `puertoServidorCentral`: Puerto del servidor central para workers (12346 por defecto)
- `puertoEscuchaTareas`: Puerto donde este worker escuchará tareas (ej: 12351, 12352)

### Pruebas

```bash
# Ejecutar suite de pruebas
python test_worker.py

# Debería mostrar:
# ✓ ¡Todas las pruebas pasaron! El worker está listo.
```

## Estructura de Datos

### Archivos de Partición

Los datos se almacenan en archivos de texto en formato CSV:

```
data_<workerId>/
├── CUENTA_P1.txt      # Datos de cuentas de la partición 1
├── CLIENTE_P1.txt     # Datos de clientes de la partición 1
└── transacciones_locales.log  # Log de transacciones locales
```

### Formato de Archivos

**CUENTA_Px.txt:**
```
ID_CUENTA|ID_CLIENTE|SALDO|TIPO_CUENTA
100001|1|5000.00|Corriente
100002|2|3000.00|Ahorros
```

**CLIENTE_Px.txt:**
```
ID_CLIENTE|NOMBRE|EMAIL|TELEFONO
1|Juan Pérez|juan@test.com|123456789
2|Ana García|ana@test.com|987654321
```

## Comunicación con Servidor Java

El worker Python utiliza un protocolo simplificado pero compatible:

1. **Registro**: Envía mensaje de registro al servidor central
2. **Recepción de datos**: Recibe y almacena datos de partición asignados
3. **Confirmación**: Confirma recepción y preparación
4. **Escucha**: Escucha tareas en puerto asignado
5. **Procesamiento**: Procesa solicitudes y envía respuestas

## Logging

El worker genera logs detallados:

```
2024-01-01 10:00:00,000 - INFO - Worker [worker_py1]: Inicializado con directorio data_worker_py1
2024-01-01 10:00:01,000 - INFO - Worker [worker_py1]: Conectado a servidor central localhost:12346
2024-01-01 10:00:02,000 - INFO - Worker [worker_py1]: Registro completado
2024-01-01 10:00:03,000 - INFO - Worker [worker_py1]: Escuchando tareas en puerto 12351
```

## Integración con Sistema Java

Para usar workers Python junto con workers Java:

1. Inicie el servidor central Java
2. Inicie workers Java (opcional)
3. Inicie workers Python con puertos diferentes
4. Ejecute cliente para probar operaciones

```bash
# Terminal 1: Servidor Central
cd centralserver
java ServidorCentral.java

# Terminal 2: Worker Java
cd workernode  
java NodoTrabajador.java worker1 localhost 12346 12350

# Terminal 3: Worker Python
cd workernode-py
python nodo_trabajador.py worker_py1 localhost 12346 12351

# Terminal 4: Cliente
cd client
java Cliente.java -h localhost -p 12345 -c 3 -o 5
```

## Limitaciones Conocidas

1. **Serialización**: Usa JSON en lugar de serialización Java nativa (puede requerir adaptaciones en el servidor)
2. **Tipos de datos**: Algunos tipos complejos de Java pueden requerir conversión manual
3. **Rendimiento**: Puede ser ligeramente más lento que la implementación Java

## Troubleshooting

### Error de conexión
```
Worker [worker_py1]: Error durante registro: [Errno 111] Connection refused
```
**Solución**: Verificar que el servidor central esté ejecutándose en el puerto correcto.

### Error de puerto en uso
```
Worker [worker_py1]: Error crítico en escucha de tareas: [Errno 98] Address already in use
```
**Solución**: Usar un puerto diferente para `puertoEscuchaTareas`.

### Error de archivo no encontrado
```
Worker [worker_py1]: Archivo de partición no encontrado
```
**Solución**: Verificar que el worker se haya registrado correctamente y recibido datos.

## Desarrollo

Para extender o modificar el worker Python:

1. Modifica `nodo_trabajador.py` para nuevas funcionalidades
2. Añade pruebas correspondientes en `test_worker.py`  
3. Ejecuta las pruebas para verificar que no se rompa nada
4. Documenta cambios en este README

## Compatibilidad

- **Python**: 3.7+
- **Servidor Java**: Compatible con la implementación actual
- **Workers Java**: Puede coexistir con workers Java
- **Clientes**: Funciona con cualquier cliente del sistema

## Próximas Mejoras

- [ ] Implementación de heartbeat con servidor central
- [ ] Serialización Java nativa usando librería externa
- [ ] Métricas de rendimiento
- [ ] Configuración via archivo
- [ ] Soporte para múltiples particiones por worker
- [ ] Backup automático de datos 
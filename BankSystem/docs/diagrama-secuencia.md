# Diagramas de Secuencia

## Operación: Consulta de Saldo

```
Cliente             ServidorCentral           NodoTrabajador
   |                      |                         |
   | CONSULTAR_SALDO(101) |                         |
   |--------------------->|                         |
   |                      | determinar_nodo()       |
   |                      |-------------------------|
   |                      |                         |
   |                      | PROCESAR_CONSULTA(101)  |
   |                      |------------------------>|
   |                      |                         |
   |                      |                         | buscar_cuenta()
   |                      |                         |
   |                      |                         | verificar_saldo()
   |                      |                         |
   |                      | RESPUESTA_CONSULTA      |
   |                      |<------------------------|
   |                      |                         |
   | RESPUESTA_SALDO      |                         |
   |<---------------------|                         |
   |                      |                         |
```

## Operación: Transferencia de Fondos

```
Cliente             ServidorCentral         NodoTrabajador1      NodoTrabajador2
   |                      |                       |                     |
   | TRANSFERIR(101,102,  |                       |                     |
   |           500.00)    |                       |                     |
   |--------------------->|                       |                     |
   |                      | determinar_nodo(101)  |                     |
   |                      |---------------------->|                     |
   |                      |                       |                     |
   |                      | VERIFICAR_SALDO       |                     |
   |                      |---------------------->|                     |
   |                      |                       |                     |
   |                      |                       | buscar_cuenta()     |
   |                      |                       |                     |
   |                      |                       | verificar_saldo()   |
   |                      |                       |                     |
   |                      | RESPUESTA_VERIFICAR   |                     |
   |                      |<----------------------|                     |
   |                      |                       |                     |
   |                      | ACTUALIZAR_SALDO(-500)|                     |
   |                      |---------------------->|                     |
   |                      |                       |                     |
   |                      |                       | debitar()           |
   |                      |                       |                     |
   |                      | RESPUESTA_ACTUALIZAR  |                     |
   |                      |<----------------------|                     |
   |                      |                       |                     |
   |                      | determinar_nodo(102)  |                     |
   |                      |---------------------------------------------->|
   |                      |                       |                     |
   |                      | ACTUALIZAR_SALDO(+500)|                     |
   |                      |---------------------------------------------->|
   |                      |                       |                     |
   |                      |                       |                     | acreditar()
   |                      |                       |                     |
   |                      | RESPUESTA_ACTUALIZAR  |                     |
   |                      |<----------------------------------------------|
   |                      |                       |                     |
   |                      | REGISTRAR_TRANSACCION |                     |
   |                      |---------------------->|                     |
   |                      |                       |                     |
   |                      |                       | registrar()         |
   |                      |                       |                     |
   |                      | RESPUESTA_REGISTRO    |                     |
   |                      |<----------------------|                     |
   |                      |                       |                     |
   | RESPUESTA_TRANSFER   |                       |                     |
   |<---------------------|                       |                     |
   |                      |                       |                     |
```

## Operación: Registro de Nodo Trabajador

```
NodoTrabajador      ServidorCentral
      |                   |
      | cargar_particiones()
      |                   |
      | conectar()        |
      |------------------>|
      |                   |
      | REGISTRO_NODO     |
      |------------------>|
      |                   |
      |                   | registrar_nodo()
      |                   |
      | CONFIRMACION      |
      |<------------------|
      |                   |
      | iniciar_procesamiento()
      |                   |
```

## Operación: Manejo de Fallo de Nodo

```
Cliente         ServidorCentral     NodoTrabajador1    NodoTrabajador2
   |                 |                    |                  |
   | CONSULTAR_SALDO |                    |                  |
   |---------------->|                    |                  |
   |                 | PROCESAR_CONSULTA  |                  |
   |                 |------------------->|                  |
   |                 |                    |                  |
   |                 |      TIMEOUT       |                  |
   |                 |<-------------------|                  |
   |                 |                    |                  |
   |                 | detectar_fallo()   |                  |
   |                 | buscar_replica()   |                  |
   |                 |                    |                  |
   |                 | PROCESAR_CONSULTA  |                  |
   |                 |---------------------------------->|  |
   |                 |                    |                  |
   |                 |                    |                  |
   |                 | RESPUESTA          |                  |
   |                 |<----------------------------------|  |
   |                 |                    |                  |
   | RESPUESTA_SALDO |                    |                  |
   |<----------------|                    |                  |
   |                 |                    |                  |
``` 
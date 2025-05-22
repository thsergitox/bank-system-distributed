#!/bin/bash
# Script para iniciar Worker Node Python
# Uso: ./start_python_worker.sh [worker_id] [server_host] [server_port] [worker_port]

# Parámetros por defecto
WORKER_ID=${1:-"worker_py1"}
SERVER_HOST=${2:-"localhost"}
SERVER_PORT=${3:-"12346"}
WORKER_PORT=${4:-"12351"}

echo "=== Iniciando Worker Node Python ==="
echo "Worker ID: $WORKER_ID"
echo "Servidor Central: $SERVER_HOST:$SERVER_PORT"
echo "Puerto Worker: $WORKER_PORT"
echo "==================================="

# Verificar que Python está disponible
if ! command -v python3 &> /dev/null; then
    echo "Error: Python 3 no está instalado o no está en PATH"
    exit 1
fi

# Verificar que el archivo principal existe
if [ ! -f "nodo_trabajador.py" ]; then
    echo "Error: nodo_trabajador.py no encontrado en el directorio actual"
    echo "Asegúrese de ejecutar este script desde el directorio workernode-py"
    exit 1
fi

# Ejecutar pruebas rápidas (opcional)
if [ "$5" = "--test" ]; then
    echo "Ejecutando pruebas..."
    python3 test_worker.py
    if [ $? -ne 0 ]; then
        echo "Las pruebas fallaron. No se iniciará el worker."
        exit 1
    fi
    echo "Pruebas exitosas. Iniciando worker..."
fi

# Iniciar el worker
echo "Iniciando worker Python..."
python3 nodo_trabajador.py "$WORKER_ID" "$SERVER_HOST" "$SERVER_PORT" "$WORKER_PORT" 
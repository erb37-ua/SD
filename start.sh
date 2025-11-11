#!/bin/bash
set -e

echo "========================================"
# Obtener IP local del host
HOST_IP=$(hostname -i | awk '{print $1}')
echo "HOST_IP = $HOST_IP"
echo "========================================"

# Exportar la variable para Docker
export HOST_IP

echo "[INFO] Iniciando contenedores con Docker Compose..."
docker compose build
docker compose up
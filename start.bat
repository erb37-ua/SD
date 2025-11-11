@echo off
REM === Obtener IP local del host ===
for /f %%i in ('powershell -NoProfile -Command "(Get-NetIPConfiguration | Where-Object { $_.IPv4DefaultGateway } | Select-Object -First 1).IPv4Address.IPAddress"') do set HOST_IP=%%i

echo.
echo ========================================
echo  HOST_IP = %HOST_IP%
echo ========================================
echo.

REM === Lanzar Docker Compose (modo detached) ===
echo [INFO] Iniciando contenedores...
cd Deploy
docker compose build
docker compose up -d
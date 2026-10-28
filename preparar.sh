#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
perfil=${1:-elt}
case "$perfil" in lake|elt|hadoop|minio|hive|trino|lab|airflow|kafka) ;; *) echo "Uso: bash preparar.sh lake|elt|hadoop|minio|hive|trino|lab|airflow|kafka"; exit 1 ;; esac
if [ "$(uname -s)" != Darwin ] || [ "$(uname -m)" != arm64 ]; then
  echo "Este asistente se ejecuta desde Terminal de macOS en Apple Silicon."
  exit 1
fi
docker info >/dev/null
docker compose version
if [ ! -f .env ]; then
  sed "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$(id -u)/" .env.example > .env
else
  echo "Se conserva .env. Comprueba AIRFLOW_UID=$(id -u)."
fi
if [ "$perfil" = kafka ]; then
  docker compose -f docker-compose.kafka.yml config -q
  docker compose -f docker-compose.kafka.yml pull
  echo "Preparado: docker compose -f docker-compose.kafka.yml up -d --wait --wait-timeout 600"
  exit 0
fi
docker compose --profile "$perfil" config -q
case "$perfil" in
  lake|trino|lab|elt|airflow) docker compose build lab ;;
esac
case "$perfil" in
  elt|airflow) docker compose build airflow-scheduler ;;
esac
docker compose --profile "$perfil" pull --ignore-buildable
echo "Preparado: docker compose --profile $perfil up -d --no-build --wait --wait-timeout 900"
case "$perfil" in
  lake|hadoop|hive|trino) echo "Este escenario requiere emulación amd64: revisa Apple Virtualization framework y Rosetta en Docker Desktop." ;;
esac

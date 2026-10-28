#!/bin/bash
set -euo pipefail
cd "$(dirname "$0")"
perfil=${1:-elt}
case "$perfil" in
  lake)
    docker compose exec -T namenode hdfs dfsadmin -report
    docker compose exec -T trino trino --file /opt/prueba-lake.sql
    comprobar_cuenta() {
      valor=$(docker compose exec -T trino trino --output-format CSV_UNQUOTED --execute "SELECT count(*) FROM $1" | tr -d '\r')
      if [ "$valor" != "$2" ]; then echo "ERROR: $1 devuelve $valor; esperado $2"; exit 1; fi
      echo "OK: $1 = $valor"
    }
    comprobar_cuenta hive.prueba_m4_hdfs.nation 25
    comprobar_cuenta hive.prueba_m4_minio.region 5
    comprobar_cuenta mysql.retail_db.orders 20000
    ;;
  elt)
    echo "Cierra las conexiones DuckDB de Jupyter y espera a que termine cualquier DAG antes de esta prueba."
    docker compose exec -T lab python /workspace/dlt/mysql_to_duckdb.py
    docker compose exec -T -w /workspace/dbt/retail lab dbt run --profiles-dir /workspace/dbt/retail
    docker compose exec -T -w /workspace/dbt/retail lab dbt test --profiles-dir /workspace/dbt/retail
    ;;
  *) echo "Uso: bash comprobar.sh lake|elt"; exit 1 ;;
esac
echo "Comprobación $perfil: OK"

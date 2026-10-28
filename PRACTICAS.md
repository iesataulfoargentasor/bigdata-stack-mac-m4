# Prácticas del stack en Mac

Completa primero la preparación del README. Usa un escenario cada vez y cierra las conexiones a DuckDB antes de lanzar el DAG.

## Recorrido 1: consultar el lago (`lake`)

```bash
docker compose --profile lake up -d --no-build --wait --wait-timeout 900
docker compose exec trino trino
```

```sql
-- Tabla Parquet en HDFS
CREATE SCHEMA hive.demo
  WITH (location = 'hdfs://namenode:9000/user/hive/warehouse/demo.db');
CREATE TABLE hive.demo.nation WITH (format = 'PARQUET') AS
  SELECT * FROM tpch.tiny.nation;
SELECT count(*) FROM hive.demo.nation;             -- 25

-- Tabla Parquet en MinIO
CREATE SCHEMA hive.minio WITH (location = 's3a://warehouse/minio.db');
CREATE TABLE hive.minio.region WITH (format = 'PARQUET') AS
  SELECT * FROM tpch.tiny.region;
SELECT count(*) FROM hive.minio.region;            -- 5

-- Federación: el lago y MySQL en la misma consulta
SELECT c.customer_city, count(*) AS pedidos
FROM mysql.retail_db.orders o
JOIN mysql.retail_db.customers c ON o.order_customer_id = c.customer_id
GROUP BY 1 ORDER BY 2 DESC LIMIT 5;
```

El catálogo es **compartido**: lo creado desde Trino se ve desde HiveServer2.

```bash
docker compose exec hiveserver2 beeline -u "jdbc:hive2://localhost:10000/"
```

```sql
SHOW DATABASES;                       -- default, demo, minio
SELECT count(*) FROM demo.nation;     -- 25 (lee de HDFS)
SELECT count(*) FROM minio.region;    -- 5  (lee de MinIO)
```

YARN sigue disponible para MapReduce clásico. Las prácticas de `notebooks/` se lanzan desde el
Jupyter del `client` (http://localhost:8889), igual que en el clúster mínimo; el `client` arranca
con el profile `hadoop` o `all`:

```bash
docker compose --profile hadoop up -d
```

---

## Recorrido 2: ELT con dlt, dbt y Airflow (`elt`)

MySQL → dlt → DuckDB (retail_raw) → dbt → vistas y tablas de análisis. Airflow coordina estos pasos.

### A mano, desde el nodo `lab`

```bash
docker compose --profile elt up -d --no-build --wait --wait-timeout 900

# EL: dlt replica mysql-retail -> DuckDB (esquema retail_raw)
docker compose exec lab python /workspace/dlt/mysql_to_duckdb.py

# T: dbt transforma de staging a marts, y comprueba los tests
docker compose exec -w /workspace/dbt/retail lab dbt run --profiles-dir /workspace/dbt/retail
docker compose exec -w /workspace/dbt/retail lab dbt test --profiles-dir /workspace/dbt/retail

# Resultado
docker compose exec lab duckdb /workspace/data/retail.duckdb \
  -c "SELECT * FROM main_marts.ventas_por_ciudad LIMIT 5;"
```

dlt **no transforma**: trae los datos tal cual. dbt **no extrae**: sólo transforma lo cargado.
Esa separación es lo que distingue ELT de ETL.

### Con Airflow

1. Abre http://localhost:8082 (`admin` / `admin`).
2. Activa el DAG **`elt_retail`** y pulsa *Trigger DAG*.
3. Verás tres tareas: `dlt_mysql_a_duckdb` → `dbt_run` → `dbt_test`.

```bash
# O desde la terminal
docker compose exec airflow-scheduler airflow dags unpause elt_retail
docker compose exec airflow-scheduler airflow dags trigger elt_retail
docker compose exec airflow-scheduler airflow dags list-runs -d elt_retail
```

> DuckDB admite **un solo proceso escribiendo**. Si un cuaderno del `lab` tiene abierto
> `retail.duckdb`, la tarea de Airflow fallará hasta que cierres la conexión.

### MinIO desde el `lab`

El `lab` trae las variables `AWS_*` apuntando a MinIO:

```bash
docker compose exec lab aws s3 ls
docker compose exec lab aws s3 cp /workspace/dbt/retail/dbt_project.yml s3://raw-data/
```

```python
import duckdb
con = duckdb.connect()
con.sql("""
  CREATE SECRET minio (TYPE s3, KEY_ID 'minioadmin', SECRET 'minioadmin',
                       ENDPOINT 'minio:9000', URL_STYLE 'path', USE_SSL false);
""")
con.sql("COPY (SELECT * FROM range(10)) TO 's3://processed/rango.parquet'")
con.sql("SELECT * FROM 's3://processed/rango.parquet'").show()
```

---

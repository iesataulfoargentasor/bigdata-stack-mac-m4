CREATE SCHEMA IF NOT EXISTS hive.prueba_m4_hdfs
  WITH (location = 'hdfs://namenode:9000/user/hive/warehouse/prueba_m4_hdfs.db');
CREATE TABLE IF NOT EXISTS hive.prueba_m4_hdfs.nation WITH (format = 'PARQUET') AS
  SELECT * FROM tpch.tiny.nation;
CREATE SCHEMA IF NOT EXISTS hive.prueba_m4_minio
  WITH (location = 's3a://warehouse/prueba_m4_minio.db');
CREATE TABLE IF NOT EXISTS hive.prueba_m4_minio.region WITH (format = 'PARQUET') AS
  SELECT * FROM tpch.tiny.region;

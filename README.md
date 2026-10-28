# Big Data en MacBook Air M4 · 24 GB

Adaptación del [stack docente Big Data del IES Ataúlfo Argenta](https://iesataulfoargentasor.github.io/bigdata-stack-docker/#mapa): HDFS, YARN, MinIO, Hive, Trino, MySQL, JupyterLab, DuckDB, dlt, dbt, Airflow y un clúster Kafka independiente.

**[Guía web](https://iesataulfoargentasor.github.io/bigdata-stack-mac-m4/)** · **[Descargar ZIP](https://github.com/iesataulfoargentasor/bigdata-stack-mac-m4/archive/refs/heads/main.zip)** · **[Prácticas](PRACTICAS.md)** · **[Validación](VALIDACION.txt)**

**Esta versión es mixta, no completamente ARM64.** Las imágenes docentes de Hadoop y Hive sólo están disponibles para Intel y se ejecutan con emulación. El resto utiliza ARM64. La adaptación no se ha ejecutado todavía en un Mac M4: las comprobaciones de configuración no sustituyen la prueba en tu equipo.

## Arquitectura

<a id="mapa"></a>

```text
MacBook Air M4 (24 GB) → Docker Desktop
│
├── lake: consultar el lago
│   ├── HDFS + YARN                 [amd64 emulado]
│   ├── Hive Metastore + HiveServer2 [amd64 emulado]
│   ├── MinIO                      [ARM64]
│   ├── MySQL catálogo + retail     [ARM64]
│   └── Trino                      [ARM64]
│
├── elt: extraer, cargar y transformar
│   ├── MySQL → dlt → DuckDB → dbt  [ARM64]
│   ├── JupyterLab + MinIO          [ARM64]
│   └── Airflow + PostgreSQL        [ARM64]
│
└── Kafka: 3 brokers + consola      [ARM64, Compose separado]
```

Lab y Airflow se construyen en tu Mac desde bases oficiales ARM64. No se descargan sus antiguas imágenes docentes Intel. Las imágenes de Hadoop/Hive se conservan con `platform: linux/amd64` explícito; su emulación puede hacer que `lake` arranque más lentamente.

Los servicios tienen nombres de proyecto propios y no fijan nombres globales de contenedor. Las interfaces se publican sólo en `127.0.0.1`. Aunque no colisionen los nombres, debes parar otros stacks que ocupen los mismos puertos, incluido el de Spark.

## 1. Preparar Docker Desktop

1. Instala [Docker Desktop para Mac con Apple Silicon](https://docs.docker.com/desktop/setup/install/mac-install/) y ábrelo.
2. Como punto de partida para tus 24 GB, asigna **14 GB de memoria y 6 CPU** a Docker. Reserva **al menos 25 GB de disco libre** para las imágenes, construcciones y datos pequeños. Son orientaciones de esta práctica, no una garantía para cualquier volumen de datos.
3. Para los escenarios con Hadoop/Hive, selecciona **Apple Virtualization framework** en la configuración del motor y activa **Use Rosetta for x86_64/amd64 emulation on Apple Silicon**. Si Docker pide instalar Rosetta, sigue su asistente. Los nombres de los ajustes pueden variar con la versión.
4. Utiliza VirtioFS para las carpetas compartidas cuando la opción esté disponible. Aplica los cambios y reinicia Docker si lo solicita.
5. Guarda el proyecto en una carpeta local, fuera de iCloud Drive. No ejecutes todos los escenarios a la vez. Empieza por `elt`, que no necesita emular Intel.

Docker documenta que Rosetta está disponible con Apple Virtualization framework y que Docker VMM actualmente no la admite. Puedes consultar los detalles en [configuración](https://docs.docker.com/desktop/settings-and-maintenance/settings/) y [motores de virtualización](https://docs.docker.com/desktop/features/vmm/). El escenario `elt` y Kafka no requieren Rosetta.

No necesitas instalar Python, Java, Hadoop ni bases de datos en macOS. Para este stack se recomienda una versión actual de Docker Desktop con Compose 2.20 o posterior.

## 2. Clonar desde Terminal

```bash
mkdir -p ~/Proyectos
cd ~/Proyectos
git clone https://github.com/iesataulfoargentasor/bigdata-stack-mac-m4.git
cd bigdata-stack-mac-m4
docker version
docker compose version
```

Si macOS pide instalar las herramientas de línea de comandos al ejecutar Git, completa la instalación y repite. Alternativamente descarga el ZIP, descomprímelo y entra en su carpeta real, que puede llamarse `bigdata-stack-mac-m4-main`.

`docker version` debe mostrar Client y Server. Si devuelve un error interno, resuelve el funcionamiento de Docker antes de continuar. Ejecuta todos los comandos siguientes desde la carpeta que contiene `compose.yaml`.

## 3. Elegir un escenario

| Escenario | Qué aprenderás | Preparación |
|---|---|---|
| `elt` | MySQL, Jupyter, DuckDB, dlt, dbt y Airflow | Nativo ARM64; recomendado para comenzar |
| `lake` | HDFS, YARN, Hive, MinIO y Trino | ARM64 + emulación Intel |
| Kafka | Mensajería, particiones y réplicas | Nativo ARM64; Compose separado |

Hay perfiles auxiliares `hadoop`, `minio`, `hive`, `trino`, `lab` y `airflow`. Hive y Trino incluyen sus dependencias. `all` se conserva para diagnóstico, pero no es la opción recomendada para el MacBook Air: iniciar todo a la vez puede agotar la memoria.

### ELT

```bash
bash preparar.sh elt
docker compose --profile elt up -d --no-build --wait --wait-timeout 900
docker compose --profile elt ps -a
```

El asistente crea `.env` con tu UID y construye Lab y Airflow para ARM64. Después descarga las imágenes oficiales necesarias. Si `.env` ya existe, lo conserva: comprueba que `AIRFLOW_UID` coincida con `id -u`. No uses `sudo` para preparar la carpeta.

Abre Jupyter en http://localhost:8888 y Airflow en http://localhost:8082 (admin/admin). MinIO está en http://localhost:9001 (minioadmin/minioadmin).

### Lago

Detén primero el escenario anterior:

```bash
docker compose --profile "*" stop
bash preparar.sh lake
docker compose --profile lake up -d --no-build --wait --wait-timeout 900
docker compose --profile lake ps -a
```

Se construye el pequeño cargador de datos usando la misma imagen ARM64 del laboratorio y se descargan las imágenes del lago. No reconstruyas los Dockerfiles de Hadoop/Hive para ARM64: esta versión está diseñada para utilizar sus imágenes Intel con emulación explícita.

### Kafka

```bash
docker compose --profile "*" stop
bash preparar.sh kafka
docker compose -f docker-compose.kafka.yml up -d --wait --wait-timeout 600
```

Abre http://localhost:8083. Cada broker tiene su volumen persistente. Los clientes ejecutados en el Mac usan localhost:29092, localhost:39092 y localhost:49092. Los clientes dentro de Kafka utilizan kafka-1:19092, kafka-2:19092 y kafka-3:19092.

## 4. Interfaces y almacenamiento

| Servicio | Puerto local | Acceso |
|---|---|---|
| NameNode | 9870 | http://localhost:9870 |
| ResourceManager | 8088 | http://localhost:8088 |
| DataNode / NodeManager | 9864 / 8042 | Interfaces del nodo |
| Trino | 8080 | http://localhost:8080 |
| HiveServer2 web / JDBC | 10002 / 10000 | Consola / JDBC |
| MinIO consola / API | 9001 / 9000 | minioadmin / minioadmin |
| JupyterLab | 8888 | Sin token, sólo acceso local |
| Cliente Hadoop | 8889 | Perfil hadoop, sin token |
| Airflow | 8082 | admin / admin |
| MySQL catálogo / retail | 3306 / 3307 | hive/hive · retail/retail |
| Kafka UI | 8083 | http://localhost:8083 |

`workspace/` guarda los scripts, los resultados DuckDB y los archivos de trabajo. `notebooks/` se comparte con el cliente Hadoop. Los datos de HDFS, MinIO, MySQL, PostgreSQL y Kafka se guardan en volúmenes Docker.

Es un entorno docente local con credenciales sencillas. No está preparado para exponer sus servicios a Internet. No necesitas configurar Tailscale para trabajar en tu propio Mac.

## 5. Comprobar el funcionamiento

Con `elt` arrancado y sin otra aplicación utilizando el fichero DuckDB:

```bash
bash comprobar.sh elt
```

Debe terminar con seis modelos de dbt creados y once pruebas correctas. En Jupyter crea y guarda un cuaderno. Después cierra cualquier conexión a DuckDB, activa `elt_retail` en Airflow y pulsa Trigger DAG. Las tres tareas deben quedar en verde. No ejecutes el flujo manual al mismo tiempo que el DAG.

Con `lake` arrancado:

```bash
bash comprobar.sh lake
```

La prueba crea tablas de ejemplo independientes y comprueba 25 filas en HDFS, 5 en MinIO y 20000 pedidos en MySQL. Se puede repetir: conserva las tablas ya creadas. Si has modificado los datos retail, el resultado esperado puede variar.

Para Kafka:

```bash
docker compose -f docker-compose.kafka.yml exec kafka-1 /opt/kafka/bin/kafka-topics.sh --bootstrap-server kafka-1:19092 --create --if-not-exists --topic prueba-m4 --partitions 1 --replication-factor 3
printf 'hola desde el M4\n' | docker compose -f docker-compose.kafka.yml exec -T kafka-1 /opt/kafka/bin/kafka-console-producer.sh --bootstrap-server kafka-1:19092 --topic prueba-m4
docker compose -f docker-compose.kafka.yml exec -T kafka-2 /opt/kafka/bin/kafka-console-consumer.sh --bootstrap-server kafka-2:19092 --topic prueba-m4 --from-beginning --max-messages 1 --timeout-ms 30000
```

Continúa con los ejercicios de [PRACTICAS.md](PRACTICAS.md): consultas compartidas entre Hive y Trino, análisis de ventas y acceso a MinIO desde DuckDB.

## 6. Parar, reanudar y conservar datos

```bash
# Parar el stack principal
docker compose --profile "*" stop
# Parar Kafka, si lo habías iniciado
docker compose -f docker-compose.kafka.yml stop
```

Para reanudar utiliza el mismo `up` del escenario elegido. Si necesitas recrear contenedores, puedes usar `down` y después `up`: los volúmenes y las carpetas compartidas se conservan. **No uses `down -v`: borra los volúmenes.**

En HDFS, el NameNode permite incorporarse al DataNode antes de que `hdfs-init` espere la salida del modo seguro y prepare las carpetas. No fuerces la salida del modo seguro como paso rutinario.

Después de preparar y comprobar un escenario con Internet, puedes repetir su `up` añadiendo `--no-build --pull never` sin conexión. Los ejemplos que descarguen datos o paquetes externos requerirán preparación adicional.

## 7. Diagnóstico

```bash
docker compose --profile "*" ps -a
docker compose logs --tail 100 NOMBRE_DEL_SERVICIO
```

- `Exited (0)` es normal en los inicializadores. `unhealthy` o una salida distinta de 0 requiere revisar los logs.
- Si Hadoop/Hive fallan o son muy lentos, revisa Rosetta, el motor de virtualización, la memoria y los logs. Una plataforma declarada no garantiza que la emulación funcione en cualquier versión de macOS.
- Si aparece un error de permisos, revisa `AIRFLOW_UID` y que Docker pueda compartir la carpeta. Una variable exportada en Terminal prevalece sobre `.env`.
- Si Docker intenta descargar `bigdata-m4-lab:1.0` o no encuentra la imagen, ejecuta de nuevo `bash preparar.sh` con el escenario correspondiente.
- Si el puerto está ocupado, para el otro stack (por ejemplo, Spark) o modifica sólo el puerto publicado en el Compose.
- Si el equipo pierde fluidez, detén los escenarios que no uses. Los contenedores Linux siguen consumiendo recursos aunque cierres la pestaña del navegador.

## 8. Actualizar

```bash
docker compose --profile "*" stop
docker compose -f docker-compose.kafka.yml stop
git pull --ff-only
bash preparar.sh elt
docker compose --profile elt up -d --no-build --wait --wait-timeout 900
```

Cambia `elt` por el escenario que uses. Una clonación nueva ya contiene la última versión. No borres tu `.env` ni tus cuadernos para actualizar.

## Procedencia y alcance

Adaptación del repositorio original, commit `aa6b050604b0a8246c646473667d858593c4885b`. Se mantienen las correcciones de arranque, la carga retail, la persistencia Kafka y el DAG con un máximo de una ejecución activa. Se cambian las plataformas, la construcción de Lab/Airflow, los nombres de proyecto y la publicación local de puertos.

El repositorio original se probó en Windows/amd64. Eso no demuestra que esta adaptación mixta funcione en el M4. Lee [VALIDACION.txt](VALIDACION.txt) para ver exactamente qué se ha comprobado. El inventario de arquitecturas está en `ARQUITECTURAS.json`.

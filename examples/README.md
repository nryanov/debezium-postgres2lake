# Examples

Docker Compose stacks that run **debezium-postgres2lake** against **PostgreSQL** (logical replication), **Apicurio Schema Registry**, and **MinIO**. Migrations use **Liquibase** (demo table, publication, replication slot). Use **one example at a time** on a developer machine so ports **5432**, **8080**, **9000**, **9090**, and (where used) **9083** / **19120** do not collide.

## Prerequisites

1. **Container images** — from the repository root:

   ```bash
   ./scripts/build-container-images.sh
   ```

   Images are tagged as `local/debezium-postgres2lake-<format>:<version>` (default tag matches the Gradle project version, e.g. `0.1.0`). Override with `IMAGE_TAG` in the environment if you build with a custom tag.

2. **Jupyter image** — the shared Compose file starts a `jupyter` service that uses a **local** image not produced by `build-container-images.sh`. Build it once from the repository root:

   ```bash
   docker build -t local/jupyter-spark:0.1.0 -f examples/common/jupyter/Dockerfile examples/common/jupyter
   ```

   The [Dockerfile](common/jupyter/Dockerfile) extends `quay.io/jupyter/pyspark-notebook` (Spark 4.1.x) and adds **psycopg**, **PyIceberg** (with Postgres/Hive extras), PostgreSQL and Hadoop **S3A** jars on Spark’s classpath, **Spark Avro**, and Iceberg/Paimon Spark runtime JARs under `/home/jovyan/extra-jars/`.

   App images honor `IMAGE_TAG`; the Jupyter service is pinned to `local/jupyter-spark:0.1.0` in [common/base-compose.yaml](common/base-compose.yaml). Use that tag when building, or change the image reference in `base-compose.yaml` to match your tag.

3. **Hive-based examples** (`iceberg-hive`, `paimon-hive`) — JDBC driver for the Hive Metastore container:

   ```bash
   bash examples/common/fetch-jars.sh
   ```

### Python scripts: fetch, generate, read

Scripts live under [common/python-scripts/](common/python-scripts/). Several use Compose **service hostnames** (`postgres`, `minio`, `hive-metastore`), so run them **on the Compose network** (for example from the `jupyter` container after copying or bind-mounting the directory), or change the connection URLs to `localhost` and exposed ports when running on the host.

| Purpose                               | How                                                                                                                                                                                        |
|---------------------------------------|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| JDBC/S3A jars for Hive Metastore      | `bash examples/common/fetch-jars.sh` — writes under `common/ext-jars/` (see prerequisite 3).                                                                                               |
| Generate CDC source traffic           | [generate_data.py](common/python-scripts/generate_data.py) inserts batches into `public.demo_orders` via psycopg. Tune `batchesCount`, `batchSize`, and `interval` at the top of the file. |
| Read Avro on MinIO (Spark)            | [read_avro.py](common/python-scripts/read_avro.py) — local Spark, `s3a://` against MinIO.                                                                                                  |
| Read Iceberg via Hive catalog (Spark) | [read_iceberg_hive.py](common/python-scripts/read_iceberg_hive.py) — needs the **iceberg-hive** stack (Hive Metastore + Iceberg).                                                          |

Example: with a stack up from an example directory, copy scripts into the Jupyter container and run (the image already includes psycopg and Spark):

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```

Use the same pattern for `read_avro.py` or `read_iceberg_hive.py` (paths under `/tmp/` or another directory inside the container).

## Layout

| Directory                         | App image                        | Notes                                                              |
|-----------------------------------|----------------------------------|--------------------------------------------------------------------|
| [iceberg-jdbc](iceberg-jdbc/)     | `debezium-postgres2lake-iceberg` | Iceberg **JDBC** catalog on DB `iceberg_catalog`                   |
| [iceberg-nessie](iceberg-nessie/) | same                             | + **Nessie**                                                       |
| [iceberg-hive](iceberg-hive/)     | same                             | + **Hive Metastore** (Thrift **9083**)                             |
| [paimon-jdbc](paimon-jdbc/)       | `debezium-postgres2lake-paimon`  | Paimon **JDBC** catalog on DB `paimon_catalog`                     |
| [paimon-hive](paimon-hive/)       | same                             | + **Hive Metastore**                                               |
| [avro](avro/)                     | `debezium-postgres2lake-avro`    | Object layout under `s3a://warehouse/postgres/public/demo_orders/` |
| [parquet](parquet/)               | `debezium-postgres2lake-parquet` | Same layout, Parquet                                               |
| [orc](orc/)                       | `debezium-postgres2lake-orc`     | Same layout, ORC                                                   |

Shared Compose fragments live under [common](common/) (`base-compose.yaml`, `hive-services.yaml`, Liquibase changelog, [python-scripts/](common/python-scripts/)).

## Run an example

```bash
cd examples/iceberg-jdbc   # or any other example directory
docker compose up -d
```

Wait until `liquibase` has finished and the **app** container is healthy. Then generate CDC traffic using [common/python-scripts/generate_data.py](common/python-scripts/generate_data.py) on the Compose network (see **Python scripts** above). For example:

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```

Adjust `batchesCount`, `batchSize`, and `interval` in the script to control load. With `debezium.engine.snapshot.mode=NO_DATA`, only **new** rows are streamed.

### Iceberg / Paimon table naming

Lake tables use database name **`default_public`** and table **`demo_orders`** (Debezium destination encoding in the application).

## Compose version

Examples use YAML **`include:`** (Docker Compose **v2.20+**). Upgrade Docker Desktop / Compose if `include` is not supported.

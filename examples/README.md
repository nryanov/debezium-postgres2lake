# Examples

Docker Compose stacks that run **debezium-postgres2lake** against **PostgreSQL** (logical replication), **Apicurio Schema Registry**, and **MinIO**. Migrations use **Liquibase** (demo table, publication, replication slot). Use **one example at a time** on a developer machine so ports **5432**, **8080**, **9000**, **9090**, and (where used) **9083** / **19120** do not collide.

## Prerequisites

1. **Container images** — from the repository root:

   ```bash
   ./scripts/build-container-images.sh
   ```

   Images are tagged as `local/debezium-postgres2lake-<format>:<version>` (default tag matches the Gradle project version, e.g. `0.1.0`). Override with `IMAGE_TAG` in the environment if you build with a custom tag.

2. **Hive-based examples** (`iceberg-hive`, `paimon-hive`) — JDBC driver for the Hive Metastore container:

   ```bash
   bash examples/common/fetch-jars.sh
   ```

3. **Python** (load generator and notebooks):

   ```bash
   pip install -r examples/common/requirements-python.txt
   ```
   
3.1. **Python** venv
```shell
python3 -m venv stash/venv
source stash/venv/bin/activate
python3 -m pip install -r examples/common/requirements-python.txt
```

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

Shared Compose fragments live under [common](common/) (`base-compose.yaml`, `hive-services.yaml`, Liquibase changelog, `scripts/generate_events.py`).

## Run an example

```bash
cd examples/iceberg-jdbc   # or any other example directory
docker compose up -d
```

Wait until `liquibase` has finished and the **app** container is healthy. Then generate CDC traffic (from the same example directory):

```bash
./scripts/load.sh --batches 5 --batch-size 2
```

`load.sh` inserts into `public.demo_orders` on the source database. With `debezium.engine.snapshot.mode=NO_DATA`, only **new** rows are streamed.

### Iceberg / Paimon table naming

Lake tables use database name **`postgres_public`** and table **`demo_orders`** (Debezium destination encoding in the application).

### Notebooks

- **Iceberg** (`iceberg-* / notebooks/query.ipynb`): PyIceberg against **localhost** ports (catalog DB, MinIO, Nessie, or HMS as appropriate).
- **Paimon** (`paimon-* / notebooks/minio_layout.ipynb`): lists MinIO keys as a lightweight check.
- **Avro / ORC**: list objects under the warehouse prefix.
- **Parquet**: list and read a file with PyArrow.

### Fresh data directory

Postgres and Hive Metastore state are in Docker volumes. To reset:

```bash
docker compose down -v
```

## Compose version

Examples use YAML **`include:`** (Docker Compose **v2.20+**). Upgrade Docker Desktop / Compose if `include` is not supported.

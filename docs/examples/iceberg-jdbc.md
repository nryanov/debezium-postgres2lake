# Example: Iceberg JDBC catalog

Runs Iceberg output with a JDBC-backed Iceberg catalog stored in PostgreSQL.

## Stack details

| Item | Value |
|---|---|
| Example directory | `examples/iceberg-jdbc` |
| Compose file | `examples/iceberg-jdbc/docker-compose.yml` |
| App image | `local/debezium-postgres2lake-iceberg:${IMAGE_TAG:-0.1.0}` |
| App config file | `examples/iceberg-jdbc/config/application.properties` |
| Extra services | Base services only (`postgres`, `schema-registry`, `minio`, `jupyter`, etc.) |

## Run

```bash
cd examples/iceberg-jdbc
docker compose up -d
```

## Generate demo traffic

```bash
docker compose cp ../common/python-scripts/generate_data.py jupyter:/tmp/generate_data.py
docker compose exec jupyter python /tmp/generate_data.py
```
